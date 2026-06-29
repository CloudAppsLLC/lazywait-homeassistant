# LazyWait — Home Assistant integration

Connect a branch's on-premise **Home Assistant** to the **LazyWait** cloud. Home
Assistant pushes presence/absence events to LazyWait and polls its branch config
— all **outbound only**. The LazyWait cloud never connects into your network.

> **Where does the pairing code go?**
> The code you see in the LazyWait dashboard (e.g. `D3MRDXZBFX`) is entered **in
> Home Assistant**, in this integration's setup screen — see step 3 below. It is
> *not* typed back into the dashboard.

---

## How pairing works (the short version)

It pairs like a streaming app pairs with a TV: the **cloud mints a code**, a
human **carries it into Home Assistant**, and HA exchanges it for a long-lived
token over one outbound call.

```
LazyWait Dashboard                         Home Assistant
─────────────────                          ──────────────
Integrations → Home Assistant
  → Connect
  → shows pairing code  ───(human carries the code)──►  Add Integration → "LazyWait"
                                                          → enter base URL + code
                                                          → HA POSTs the code to /pair
  status flips to "Connected" ◄──(cloud consumes code,──  ◄ cloud returns a token
                                  mints token)             HA stores it encrypted, pings
```

The code is **single-use** and expires in **~10 minutes**. The token HA receives
is stored encrypted in HA and never shown again.

---

## Installation

### Option A — HACS (recommended)
1. In HACS → Integrations → ⋮ → **Custom repositories**, add this repo's URL,
   category **Integration**.
2. Install **LazyWait**, then restart Home Assistant.

### Option B — Manual
1. Copy `custom_components/lazywait/` into your HA `config/custom_components/`
   directory (so you have `config/custom_components/lazywait/manifest.json`).
2. Restart Home Assistant.

---

## Setup / pairing (step by step)

1. **In the LazyWait dashboard:** go to **Integrations → Home Assistant →
   Connect**. A pairing code appears with a countdown (valid ~10 min). Keep it
   on screen.
2. **In Home Assistant:** **Settings → Devices & Services → Add Integration →
   search "LazyWait"**.
3. **Enter two things** (this is the screen the code goes into):
   - **LazyWait cloud URL** — prefilled to `https://apiv2.lazywait.com/v1`. Leave
     it unless your partner gave you a different host.
   - **Pairing code** — the code from step 1 (e.g. `D3MRDXZBFX`).
4. Submit. HA redeems the code, stores the token, and finishes. Back in the
   dashboard the integration flips to **Connected**.

If the code expired before you submitted it, click **Rotate** (or **Connect**
again) in the dashboard for a fresh code.

---

## What you get in Home Assistant

A **LazyWait Branch** device with diagnostic entities:
- **Cloud connection** (binary sensor) — on while the cloud link is healthy.
- **Config version** (sensor) — the branch config version currently applied.

## Sending events to LazyWait

Presence/absence **decisions are made locally** in Home Assistant (automations,
templates, or a future helper). Hand a decided event to the coordinator's
`queue_event(...)`; the coordinator batches and flushes it to the cloud on the
next poll. The cloud applies the tenant's notification preferences and fans out
the alert (in-app / SMS / WhatsApp / email) — you do **not** pick the channel
here.

Event shape pushed to the cloud:
```json
{ "type": "absence", "entityId": "binary_sensor.front_door",
  "occurredAt": "2026-06-29T09:00:00Z", "payload": { } }
```
`type` is one of `absence` | `presence` | `device_state`.

---

## Re-pairing (token rotated or revoked)

If an admin clicks **Rotate** or **Disconnect** in the dashboard, the stored
token stops working. HA notices on its next call (a `401`) and starts a
**re-authentication** flow that asks for a **fresh pairing code** — generate one
in the dashboard and enter it. The branch stays the same; only the token is
replaced.

---

## Cloud endpoints used (reference)

All under the configured base URL + `/integrations/home-assistant`:

| Method | Path | Auth | Purpose |
|--------|------|------|---------|
| POST | `/pair` | the pairing code | redeem code → token + config |
| GET | `/config` | bearer | poll branch config (versioned) |
| POST | `/events` | bearer | push a batch of events (Idempotency-Key) |
| GET | `/ping` | bearer | liveness + token check |
| POST | `/status` | bearer | self-reported health heartbeat |

The bearer is the token minted by `/pair`. The cloud resolves your branch from
the token — Home Assistant never sends the branch id in a request body.
