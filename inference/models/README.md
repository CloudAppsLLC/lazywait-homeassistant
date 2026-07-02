# Bundled ONNX models

Place the exported YOLO11n models here (they ship inside the add-on image so the
Pi runs offline):

- `yolo11n.onnx` — detection (person + COCO objects). ~6 MB.
- `yolo11n-pose.onnx` — pose (17 keypoints), for actions/fall. ~6 MB.

Export from Ultralytics (run once, commit the .onnx files or fetch at build):

```bash
pip install ultralytics
yolo export model=yolo11n.pt      format=onnx imgsz=640 opset=12
yolo export model=yolo11n-pose.pt format=onnx imgsz=640 opset=12
mv yolo11n.onnx yolo11n-pose.onnx <this dir>
```

The detector (`detector.py`) reads them from `LW_MODELS_DIR` (default
`/app/inference/models` in the add-on image). If a model is missing the engine
reports `ready=false` and inference is skipped cleanly — snapshot + streaming are
unaffected.

> Models are NOT committed to git (binary). They're added at image-build time
> (CI step or a pre-build fetch). Until they're present, the add-on runs without
> detection and camerai.py logs "inference engine not ready".
