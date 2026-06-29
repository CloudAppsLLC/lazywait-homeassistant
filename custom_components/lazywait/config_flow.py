"""Config flow for the LazyWait integration.

THIS is where the pairing code from the LazyWait dashboard gets entered.

Flow:
  1. The branch admin opens the LazyWait dashboard → Integrations → Home
     Assistant → Connect, which shows a short-lived pairing code (e.g.
     "D3MRDXZBFX").
  2. In Home Assistant: Settings → Devices & Services → Add Integration →
     "LazyWait". This flow asks for the cloud base URL (prefilled) and that
     pairing code.
  3. We POST the code to /pair. The cloud validates it, mints a long-lived
     branch bearer token, and returns it + the branch id + initial config.
  4. We store the token (HA encrypts config-entry data at rest) and finish.
     The code is single-use and now consumed; the token is never shown again.

A later 401 (token rotated/revoked from the dashboard) triggers `async_step_
reauth`, which re-prompts for a fresh pairing code without losing the entry.
"""

from __future__ import annotations

import logging
from typing import Any

import voluptuous as vol

from homeassistant.config_entries import ConfigFlow
from homeassistant.helpers.aiohttp_client import async_get_clientsession

# ConfigFlowResult was added in HA 2024.4; older cores have FlowResult instead.
# Import whichever exists so the module loads across HA versions (a hard import
# of a missing name makes the whole config flow fail to load -> a 500 when the
# user opens the integration). We only use it as a return-type annotation.
try:  # HA >= 2024.4
    from homeassistant.config_entries import ConfigFlowResult
except ImportError:  # older HA
    try:
        from homeassistant.data_entry_flow import FlowResult as ConfigFlowResult
    except ImportError:  # extremely old fallback — annotation only
        ConfigFlowResult = Any  # type: ignore[assignment,misc]

from .api import (
    LazyWaitApiClient,
    LazyWaitApiError,
    LazyWaitPairingError,
)
from .const import (
    CONF_BASE_URL,
    CONF_BRANCH_ID,
    CONF_PAIRING_CODE,
    CONF_TOKEN,
    DEFAULT_BASE_URL,
    DOMAIN,
)

_LOGGER = logging.getLogger(__name__)

# Maps the cloud's pairing errorKeys to the translation keys in strings.json so
# the form shows an actionable, localized reason rather than a raw code.
_PAIRING_ERROR_TO_FORM = {
    "HA_CODE_EXPIRED": "code_expired",
    "HA_CODE_USED": "code_used",
    "HA_CODE_INVALID": "code_invalid",
    "HA_PAIR_BODY_INVALID": "code_invalid",
    "HA_PAIR_FAILED": "cannot_connect",
}


class LazyWaitConfigFlow(ConfigFlow, domain=DOMAIN):
    """Handle the LazyWait pairing config flow."""

    VERSION = 1

    # NOTE: do NOT define __init__ to set self._reauth_entry_id — newer HA's
    # ConfigFlow base class exposes that as a read-only property, so assigning it
    # raises "AttributeError: ... has no setter" and the flow 500s on open. We
    # read the entry id from self.context (which HA populates on reauth) instead.

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Initial step: ask for the base URL + pairing code, then redeem it."""
        errors: dict[str, str] = {}

        if user_input is not None:
            base_url = (user_input.get(CONF_BASE_URL) or DEFAULT_BASE_URL).strip()
            pairing_code = (user_input.get(CONF_PAIRING_CODE) or "").strip()

            result = await self._redeem(base_url, pairing_code, errors)
            if result is not None:
                branch_id = result.get("branchId") or ""
                token = result.get("enrollmentToken") or ""
                # The cloud echoes the authoritative base URL it wants HA to
                # use; prefer it over what the admin typed.
                resolved_base = (result.get("baseUrl") or base_url).strip()

                # One config entry per branch — block a duplicate pairing of the
                # same branch into the same HA instance.
                await self.async_set_unique_id(branch_id)
                self._abort_if_unique_id_configured()

                return self.async_create_entry(
                    title=f"LazyWait — {branch_id}",
                    data={
                        CONF_BASE_URL: resolved_base,
                        CONF_TOKEN: token,
                        CONF_BRANCH_ID: branch_id,
                    },
                )

        return self.async_show_form(
            step_id="user",
            data_schema=vol.Schema(
                {
                    vol.Required(
                        CONF_BASE_URL, default=DEFAULT_BASE_URL
                    ): str,
                    vol.Required(CONF_PAIRING_CODE): str,
                }
            ),
            errors=errors,
        )

    async def async_step_reauth(
        self, entry_data: dict[str, Any]
    ) -> ConfigFlowResult:
        """Entry point when a stored token is rejected (rotated/revoked)."""
        return await self.async_step_reauth_confirm()

    async def async_step_reauth_confirm(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Re-prompt for a fresh pairing code and replace the stored token."""
        errors: dict[str, str] = {}
        # HA puts the entry id on the reauth context; read it from there rather
        # than a custom instance attribute.
        reauth_entry_id = self.context.get("entry_id")
        entry = (
            self.hass.config_entries.async_get_entry(reauth_entry_id)
            if reauth_entry_id
            else None
        )
        base_url_default = (
            entry.data.get(CONF_BASE_URL) if entry else DEFAULT_BASE_URL
        ) or DEFAULT_BASE_URL

        if user_input is not None and entry is not None:
            base_url = (user_input.get(CONF_BASE_URL) or base_url_default).strip()
            pairing_code = (user_input.get(CONF_PAIRING_CODE) or "").strip()

            result = await self._redeem(base_url, pairing_code, errors)
            if result is not None:
                branch_id = result.get("branchId") or entry.data.get(CONF_BRANCH_ID)
                # A re-pair must stay on the same branch — refuse a code that
                # pairs a different branch into this existing entry.
                if branch_id and branch_id != entry.data.get(CONF_BRANCH_ID):
                    errors["base"] = "branch_mismatch"
                else:
                    self.hass.config_entries.async_update_entry(
                        entry,
                        data={
                            **entry.data,
                            CONF_BASE_URL: (result.get("baseUrl") or base_url).strip(),
                            CONF_TOKEN: result.get("enrollmentToken") or "",
                        },
                    )
                    await self.hass.config_entries.async_reload(entry.entry_id)
                    return self.async_abort(reason="reauth_successful")

        return self.async_show_form(
            step_id="reauth_confirm",
            data_schema=vol.Schema(
                {
                    vol.Required(CONF_BASE_URL, default=base_url_default): str,
                    vol.Required(CONF_PAIRING_CODE): str,
                }
            ),
            errors=errors,
        )

    async def _redeem(
        self, base_url: str, pairing_code: str, errors: dict[str, str]
    ) -> dict[str, Any] | None:
        """Redeem a pairing code, populating `errors` on failure.

        Returns the /pair payload on success, else None (errors set in place).
        """
        if not pairing_code:
            errors["base"] = "code_invalid"
            return None

        session = async_get_clientsession(self.hass)
        client = LazyWaitApiClient(base_url, session)
        instance_name = self.hass.config.location_name or "Home Assistant"

        try:
            payload = await client.redeem_pairing_code(pairing_code, instance_name)
        except LazyWaitPairingError as err:
            errors["base"] = _PAIRING_ERROR_TO_FORM.get(err.error_key, "cannot_connect")
            return None
        except LazyWaitApiError as err:
            _LOGGER.warning("LazyWait pairing failed: %s", err)
            errors["base"] = "cannot_connect"
            return None

        if not payload.get("enrollmentToken") or not payload.get("branchId"):
            errors["base"] = "cannot_connect"
            return None

        return payload
