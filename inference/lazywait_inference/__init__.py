"""LazyWait edge camera-AI inference (runs on the branch Raspberry Pi).

Standalone process bundled in the HA add-on image; not part of HA core (heavy ML
deps stay out of the integration, which declares requirements: []). See
../README.md and docs/camera-ai-inference-worker.md (edge-only section).
"""

__all__ = ["engine", "detector", "actions"]
