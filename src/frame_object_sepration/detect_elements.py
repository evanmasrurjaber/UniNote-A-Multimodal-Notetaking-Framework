"""
detect_single_video.py
----------------------
Run YOLO detection on keyframes for one video_id or all videos under a
keyframes root, and generate parser-ready all_detections.json + crop images.

This script is Step 1 before run_parsers.py.

Usage examples:
  # Single video
  python src/frame_extraction/detect_single_video.py \
      --video_id a0406547135d

  # Single video with custom paths
  python src/frame_extraction/detect_single_video.py \
      --video_id a0406547135d \
      --weights Dataset/data/yolo_model/weights.v1.pt \
      --keyframes_root Dataset/data/keyframes \
      --output_root data/detections \
      --conf 0.25

  # Batch mode: process all video folders in keyframes_root
  python src/frame_extraction/detect_single_video.py \
      --batch

  # Batch mode with custom paths
  python src/frame_extraction/detect_single_video.py \
      --batch \
      --keyframes_root Dataset/data/keyframes \
      --output_root data/detections
"""

import argparse
import json
from collections import Counter
from pathlib import Path

from PIL import Image
from ultralytics import YOLO

# Parsers currently support these classes only.
SUPPORTED_CLASSES = {"diagram", "equation", "graph", "table", "text"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Detect visual objects for one video or for all video folders under keyframes_root "
            "and export parser-ready detections JSON."
        )
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--video_id", help="Video id, e.g. a0406547135d")
    group.add_argument(
        "--batch",
        action="store_true",
        help="Run detection for all video subfolders in keyframes_root",
    )
    parser.add_argument(
        "--weights",
        default="Dataset/data/yolo_model/weights.v1.pt",
        help="Path to YOLO weights file",
    )
    parser.add_argument(
        "--keyframes_root",
        default="Dataset/data/keyframes",
        help="Root folder containing per-video keyframe subfolders",
    )
    parser.add_argument(
        "--output_root",
        default="data/detections",
        help="Root folder to store detection outputs",
    )
    parser.add_argument(
        "--conf",
        type=float,
        default=0.25,
        help="Detection confidence threshold",
    )
    parser.add_argument(
        "--img_exts",
        nargs="+",
        default=[".jpg", ".jpeg", ".png"],
        help="Allowed image file extensions",
    )
    return parser.parse_args()


def load_timestamp_map(metadata_path: Path) -> dict[str, float]:
    metadata = json.loads(metadata_path.read_text())
    timestamp_map: dict[str, float] = {}

    for item in metadata.get("keyframes", []):
        name = item.get("filename")
        ts = item.get("timestamp")
        if name is not None and ts is not None:
            timestamp_map[name] = float(ts)

    for item in metadata.get("keyframes_for_labeling", []):
        name = item.get("filename")
        ts = item.get("timestamp")
        if name is not None and ts is not None and name not in timestamp_map:
            timestamp_map[name] = float(ts)

    return timestamp_map


def iter_keyframe_images(video_keyframes_dir: Path, img_exts: list[str]) -> list[Path]:
    normalized_exts = {ext.lower() for ext in img_exts}
    return sorted(
        [
            p
            for p in video_keyframes_dir.iterdir()
            if p.is_file() and p.suffix.lower() in normalized_exts
        ]
    )


def process_video(
    model: YOLO,
    video_id: str,
    keyframes_root: Path,
    output_root: Path,
    conf: float,
    img_exts: list[str],
) -> tuple[int, Counter]:
    video_keyframes_dir = keyframes_root / video_id
    metadata_path = video_keyframes_dir / "extraction_metadata.json"

    output_dir = output_root / video_id
    crops_dir = output_dir / "crops"
    detections_path = output_dir / "all_detections.json"

    if not video_keyframes_dir.exists():
        raise FileNotFoundError(f"Video keyframes folder not found: {video_keyframes_dir}")
    if not metadata_path.exists():
        raise FileNotFoundError(f"Metadata not found: {metadata_path}")

    output_dir.mkdir(parents=True, exist_ok=True)
    crops_dir.mkdir(parents=True, exist_ok=True)

    timestamp_map = load_timestamp_map(metadata_path)
    images = iter_keyframe_images(video_keyframes_dir, img_exts)
    if not images:
        raise RuntimeError(f"No keyframe images found in: {video_keyframes_dir}")

    detections: list[dict] = []
    class_counter: Counter = Counter()

    print(f"Running detection on {len(images)} keyframes for video_id={video_id}")
    for image_path in images:
        timestamp = timestamp_map.get(image_path.name, 0.0)
        result = model.predict(source=str(image_path), conf=conf, verbose=False)[0]

        if result.boxes is None or len(result.boxes) == 0:
            continue

        image = Image.open(image_path).convert("RGB")
        width, height = image.size

        for idx, box in enumerate(result.boxes):
            class_id = int(box.cls.item())
            class_name = str(model.names[class_id])
            if class_name not in SUPPORTED_CLASSES:
                continue

            x1, y1, x2, y2 = box.xyxy[0].tolist()
            x1, y1 = max(0, int(x1)), max(0, int(y1))
            x2, y2 = min(width, int(x2)), min(height, int(y2))
            if x2 <= x1 or y2 <= y1:
                continue

            detection_id = f"{video_id}_t{timestamp:.2f}_{class_name}_{idx}"
            crop_path = crops_dir / f"{detection_id}.jpg"

            crop = image.crop((x1, y1, x2, y2))
            crop.save(crop_path, quality=95)

            conf_score = float(box.conf.item()) if box.conf is not None else None
            detections.append(
                {
                    "detection_id": detection_id,
                    "class_name": class_name,
                    "crop_path": str(crop_path),
                    "video_id": video_id,
                    "frame_timestamp": timestamp,
                    "source_frame": image_path.name,
                    "bbox_xyxy": [x1, y1, x2, y2],
                    "confidence": conf_score,
                }
            )
            class_counter[class_name] += 1

    detections_path.write_text(json.dumps(detections, indent=2))
    print(f"Saved detections: {detections_path}")
    print(f"Total parser-ready detections ({video_id}): {len(detections)}")
    print(f"Per-class counts ({video_id}): {dict(class_counter)}")
    return len(detections), class_counter


def resolve_video_ids(args: argparse.Namespace) -> list[str]:
    if args.video_id:
        return [args.video_id]

    keyframes_root = Path(args.keyframes_root)
    if not keyframes_root.exists():
        raise FileNotFoundError(f"Keyframes root not found: {keyframes_root}")

    video_ids = sorted(
        [
            p.name
            for p in keyframes_root.iterdir()
            if p.is_dir() and (p / "extraction_metadata.json").exists()
        ]
    )
    if not video_ids:
        raise RuntimeError(
            f"No video folders with extraction_metadata.json found in: {keyframes_root}"
        )
    return video_ids


def main() -> None:
    args = parse_args()

    weights_path = Path(args.weights)
    keyframes_root = Path(args.keyframes_root)
    output_root = Path(args.output_root)

    if not weights_path.exists():
        raise FileNotFoundError(f"Weights not found: {weights_path}")
    if not keyframes_root.exists():
        raise FileNotFoundError(f"Keyframes root not found: {keyframes_root}")

    model = YOLO(str(weights_path))
    video_ids = resolve_video_ids(args)

    print(f"Supported parser classes: {sorted(SUPPORTED_CLASSES)}")
    if args.batch:
        print(f"Batch mode enabled: found {len(video_ids)} videos under {keyframes_root}")

    total_detections = 0
    aggregate_counter: Counter = Counter()

    for video_id in video_ids:
        video_total, class_counter = process_video(
            model=model,
            video_id=video_id,
            keyframes_root=keyframes_root,
            output_root=output_root,
            conf=args.conf,
            img_exts=args.img_exts,
        )
        total_detections += video_total
        aggregate_counter.update(class_counter)

    if args.batch:
        print(f"Batch complete. Total parser-ready detections: {total_detections}")
        print(f"Batch per-class counts: {dict(aggregate_counter)}")


if __name__ == "__main__":
    main()
