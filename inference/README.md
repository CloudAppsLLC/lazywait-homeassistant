# LazyWait Edge Camera-AI Inference

Runs on the branch **Raspberry Pi 5** (inside the HA add-on image, as a
standalone process — NOT in HA core). Reads each AI-enabled camera's **local NVR
RTSP**, runs YOLO11n detection + YOLO11n-pose, tracks people, classifies
actions/falls, and batch-POSTs events to the cloud. **No central GPU; video never
leaves the branch.** See `docs/camera-ai-inference-worker.md` (edge-only section)
in LazyWaitInternalAPI for the architecture rationale.

## Why edge (not a central VPS worker)
The fleet is thousands of HA boxes × 1-8 cameras = tens of thousands of cameras.
Centralizing inference would need a GPU farm. Each Pi 5 (quad A76, 8 GB) runs its
own inference cheaply, so cost scales to $0 centrally and the fleet scales
horizontally by construction.

## What it does
- **Detection** (`detector.py`): YOLO11n via ONNX Runtime. Person + configured
  object classes (table/chair/…). Auto-selects a Hailo/Coral EP if present, else
  CPU (bounded threads so it shares the Pi with HA + ffmpeg).
- **Pose → actions** (`actions.py`): standing/sitting/raising_hand (single-frame
  geometry on 17 COCO keypoints) + walking/waving/falling (temporal, per-track
  history). Explainable rules, no extra model.
- **Engine** (`engine.py`): round-robin one camera per tick (bounded global fps),
  lightweight IoU tracker for stable track_ids, state-change event dedup
  (cooldown), slow object census, immediate fall alerts.
- **Entrypoint** (`__main__.py`): polls cloud `/config` for the `cameraAi` block,
  resolves RTSP from an env template, drives the loop, POSTs to
  `/v1/integrations/camera-events/batch` (media-relay-token auth).

## Run (dev)
```
LW_BASE_URL=https://apiv2.lazywait.com/v1 \
LW_TOKEN=<branch bearer> \
LW_RTSP_TEMPLATE='rtsp://user:pass@192.168.1.250:554/Streaming/Channels/{channel}01' \
python -m lazywait_inference
```
`{channel}` is filled from the camera id (`…channel_<N>` → `<N>`). Models live at
`LW_MODELS_DIR` (default `/app/inference/models`, bundled in the add-on image).

## Env
| var | default | meaning |
|-----|---------|---------|
| `LW_BASE_URL` | apiv2…/v1 | cloud API base (config poll) |
| `LW_TOKEN` | — | branch bearer for the config poll |
| `LW_RTSP_TEMPLATE` | — | local NVR RTSP with `{channel}` |
| `LW_MODELS_DIR` | /app/inference/models | ONNX model dir |
| `LW_TICK_HZ` | 4 | total inference passes/sec (÷ cameras = per-cam fps) |
| `LW_INFER_THREADS` | 2 | ONNX intra-op threads (leave cores for HA) |
| `LW_CONFIG_POLL_S` | 30 | config refresh interval |

Config/ingest URL + token come from the cloud `cameraAi` block, so the process
needs no per-camera setup — it reconciles automatically when an admin enables a
camera in the dashboard.

## Models
`models/yolo11n.onnx` + `models/yolo11n-pose.onnx` (~6 MB each, export from
Ultralytics `yolo export format=onnx`). Bundled in the add-on image so the Pi
works offline. Not committed here — added at image-build time.
