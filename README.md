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

## Face attendance (Hikvision)

A **Hikvision** camera at the entrance can clock employees **in/out by face** —
no badge, no app. When the camera sees a face, Home Assistant grabs a still and
forwards it to LazyWait, which recognises the employee (AWS Rekognition), toggles
their clock **IN → OUT → IN**, and writes an attendance row. A built-in **5-minute
per-employee cooldown** stops a person lingering in frame from double-punching.

These camera check-ins show up on the dashboard attendance monitor like any other
row — they're tagged `location_in.address = "hikvision:<branch_id>"` and have a
**null** `created_by_user_id` (that's how the dashboard tells a face check-in from
a manual one).

### 1. Add the camera to Home Assistant

Add your Hikvision camera in HA the normal way (the built-in **Generic Camera** or
the **ONVIF** integration, pointed at the camera's snapshot/RTSP URL). You'll need
an ISAPI user on the camera with preview/picture rights.

The still LazyWait uses is the standard ISAPI snapshot:

```
http://<user>:<pass>@<camera-host>/ISAPI/Streaming/channels/101/picture
```

`101` is channel 1, main stream (on an NVR it's `<channel>01`). Confirm it returns
a JPEG in a browser before wiring the automation.

### 2. Trigger on a face/person detection

Point an automation at the camera's own detection event so a face is actually in
frame when the snapshot is taken. Either:

- **On-device smart event** — Hikvision face/line/VMD events surfaced via the
  ONVIF or Hikvision integration as a `binary_sensor` (preferred — fires only when
  a person is seen), or
- **Motion** at the entrance as a simpler fallback.

When that trigger fires, call the bridge with the camera's host + ISAPI
credentials. The component captures the snapshot and posts it to LazyWait:

```yaml
# Example: clock employees in/out when the entrance camera detects a face.
automation:
  - alias: "LazyWait face check-in (entrance)"
    trigger:
      - platform: state
        entity_id: binary_sensor.entrance_cam_face_detection
        to: "on"
    action:
      - service: python_script.lazywait_face_checkin   # thin wrapper, see below
        data:
          host: "192.168.1.64"
          username: "attendance"
          password: !secret hikvision_attendance_pw
```

Under the hood the bridge runs:

```python
from custom_components.lazywait.hikvision import async_handle_face_event

# entry = the paired LazyWait config entry; branch is read from it automatically.
await async_handle_face_event(
    hass, entry,
    host="192.168.1.64", username="attendance", password="…",
)
# → captures the snapshot, base64-encodes it, and POSTs to
#   {base_url}/hrm/attendance/face-checkin with source="hikvision".
```

`async_handle_face_event` also accepts a pre-captured `image_base64=` if your
automation already has the frame (e.g. from an HA camera entity or a smart-event
payload) — in that case no snapshot is taken.

### What comes back

The cloud returns `{ matched, employeeName?, action: "clock_in" | "clock_out",
recorded, reason?, attendance? }`. A missed/blurred frame or an unrecognised face
simply returns `matched: false` and is dropped — the next detection tries again.
Nothing the camera does can break the automation or the cloud link.

> **Preferred future path:** Hikvision can stream the *exact cropped face* over
> `/ISAPI/Event/notification/alertStream` (its smart-event channel), avoiding the
> "whoever's in frame" snapshot. That's documented in `hikvision.py` as the next
> upgrade; the snapshot path ships today because it works on every model.

---

## Re-pairing (token rotated or revoked)

If an admin clicks **Rotate** or **Disconnect** in the dashboard, the stored
token stops working. HA notices on its next call (a `401`) and starts a
**re-authentication** flow that asks for a **fresh pairing code** — generate one
in the dashboard and enter it. The branch stays the same; only the token is
replaced.

---

## Live camera view

The dashboard can open a **live WebRTC view** of a branch camera, streamed
straight from the camera to the browser — the LazyWait cloud only relays the
handshake, never the video.

**How it works (WebRTC, NAT-friendly):**

1. In the dashboard, an admin opens a branch camera. The browser creates a
   WebRTC **offer** (an SDP blob with its ICE candidates bundled in) and POSTs
   it to the cloud, which returns a `sessionId` and **Twilio TURN** credentials
   (the same TURN service screen-share uses — reused for NAT traversal).
2. Home Assistant is **outbound-only** behind NAT, so it can't accept an inbound
   connection. Instead, on each poll cycle the integration **polls** the cloud
   for a pending offer for its branch.
3. When an offer is pending, HA asks its **bundled go2rtc** to produce an
   **answer** for that offer against the requested camera stream, and POSTs the
   answer back to the cloud.
4. The dashboard polls for the answer, applies it, and the browser and go2rtc
   connect **peer-to-peer** over Twilio TURN. **Video never touches the cloud** —
   only the SDP offer/answer does.

**What you need:**

- go2rtc is bundled in modern Home Assistant. Your camera must be published as a
  go2rtc stream (HA registers `camera.*` entities with go2rtc automatically; you
  can also name streams in `go2rtc.yaml`). The dashboard's `cameraId` maps to the
  go2rtc `src` stream name (empty → the integration's default stream).
- No inbound port-forwarding. HA stays outbound-only; the cloud is the rendezvous
  for signaling and Twilio handles media relay.

**go2rtc handshake note:** the integration tries the standalone go2rtc API
(`POST http://127.0.0.1:1984/api/webrtc?src=<stream>`) and the HA-proxied form
(`POST /api/go2rtc/webrtc?src=<stream>`), using the first that returns an SDP
answer. The exact go2rtc WebRTC route varies between go2rtc releases — if neither
answers, the integration logs **"go2rtc handshake unconfirmed"** with what it
tried, and the stream falls back gracefully rather than failing the poll loop.
See `custom_components/lazywait/camera.py` for the one call to verify against your
HA build's go2rtc.

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
| GET | `/camera/poll` | bearer | claim a pending live-camera WebRTC offer |
| POST | `/camera/answer` | bearer | return the SDP answer for a session |

The bearer is the token minted by `/pair`. The cloud resolves your branch from
the token — Home Assistant never sends the branch id in a request body.

One more endpoint lives **outside** the `/integrations/home-assistant` prefix —
the shared, device-facing face-attendance route:

| Method | Path | Auth | Purpose |
|--------|------|------|---------|
| POST | `/hrm/attendance/face-checkin` | none (public) | recognise a face → clock in/out |

Body: `{ photo_base64, branch_id?, source: "hikvision" }`. It's public because
cameras/devices hit it directly; Home Assistant still attaches its bearer (the
route ignores it). Attendance rows it writes are read back by the dashboard via
`GET /hrm/attendance`.
