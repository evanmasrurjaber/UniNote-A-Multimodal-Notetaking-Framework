#!/usr/bin/env python3
"""Batch keyframe extraction using adaptive auto-calibration.

By default this script keeps only the final label-ready keyframes
(`keyframes_for_labeling`) and deletes pruned frame images.
"""

import argparse
import json
from pathlib import Path

from keyframe_extractor import SmartWhiteboardExtractor


def keep_only_labeling_keyframes(video_output_dir):
    """Delete frame images that are not in keyframes_for_labeling."""
    metadata_path = video_output_dir / "extraction_metadata.json"
    if not metadata_path.exists():
        return 0, 0

    with open(metadata_path, "r", encoding="utf-8") as f:
        metadata = json.load(f)

    labeling_keyframes = metadata.get("keyframes_for_labeling", [])
    keep_filenames = {kf.get("filename") for kf in labeling_keyframes if kf.get("filename")}

    deleted_count = 0
    total_jpg = 0
    for image_path in video_output_dir.glob("*.jpg"):
        total_jpg += 1
        if image_path.name not in keep_filenames:
            image_path.unlink(missing_ok=True)
            deleted_count += 1

    return total_jpg, deleted_count


def parse_args():
    parser = argparse.ArgumentParser(description="Batch adaptive keyframe extraction")
    parser.add_argument(
        "--collection-log",
        default="data/raw_videos/collection_log.json",
        help="Path to collection_log.json",
    )
    parser.add_argument(
        "--videos-dir",
        default="data/raw_videos/videos",
        help="Directory containing input videos",
    )
    parser.add_argument(
        "--output-root",
        default="data/keyframes",
        help="Root output directory for per-video keyframes",
    )
    parser.add_argument(
        "--aggressiveness",
        type=float,
        default=0.35,
        help="Capture aggressiveness in [0,1], lower means fewer captures",
    )
    parser.add_argument(
        "--warmup-samples",
        type=int,
        default=80,
        help="Number of sampled frames for auto-calibration",
    )
    parser.add_argument(
        "--prune-unique-ratio",
        type=float,
        default=0.03,
        help="Minimum unique content ratio to keep an earlier keyframe",
    )
    parser.add_argument(
        "--prune-similarity-threshold",
        type=float,
        default=0.92,
        help="Drop earlier frame when very similar to a later kept frame",
    )
    parser.add_argument(
        "--video-ids",
        nargs="*",
        default=None,
        help="Optional subset of video_id values to process",
    )
    parser.add_argument(
        "--keep-pruned-images",
        action="store_true",
        help="Keep all extracted images (do not delete pruned frames)",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    collection_path = Path(args.collection_log)
    videos_dir = Path(args.videos_dir)
    output_root = Path(args.output_root)
    output_root.mkdir(parents=True, exist_ok=True)

    with open(collection_path, "r", encoding="utf-8") as f:
        metadata = json.load(f)

    selected = set(args.video_ids) if args.video_ids else None
    total_processed = 0
    total_failed = 0
    total_extracted = 0
    total_labeling = 0
    total_deleted = 0

    for video in metadata.get("videos", []):
        video_id = video.get("video_id")
        if selected and video_id not in selected:
            continue

        filename = video.get("filename")
        if not filename:
            print(f"Skipping video with missing filename: {video_id}")
            total_failed += 1
            continue

        video_path = videos_dir / filename
        output_dir = output_root / video_id

        print(f"\nProcessing: {video.get('title', video_id)}")
        print(f"Video ID: {video_id}")

        extractor = SmartWhiteboardExtractor(
            auto_calibrate=True,
            warmup_samples=args.warmup_samples,
            aggressiveness=args.aggressiveness,
            prune_for_labeling=True,
            prune_unique_ratio=args.prune_unique_ratio,
            prune_similarity_threshold=args.prune_similarity_threshold,
        )

        try:
            extracted = extractor.process_video(video_path, output_dir)
            total_extracted += len(extracted)

            metadata_path = output_dir / "extraction_metadata.json"
            with open(metadata_path, "r", encoding="utf-8") as f:
                extraction_meta = json.load(f)

            label_ready = extraction_meta.get("keyframes_for_labeling", [])
            total_labeling += len(label_ready)

            if args.keep_pruned_images:
                print(f"Success: {len(extracted)} extracted, {len(label_ready)} label-ready")
            else:
                total_jpg, deleted = keep_only_labeling_keyframes(output_dir)
                total_deleted += deleted
                print(
                    "Success: "
                    f"{len(extracted)} extracted, {len(label_ready)} label-ready, "
                    f"deleted {deleted}/{total_jpg} pruned images"
                )

            total_processed += 1
        except Exception as e:
            print(f"Error: {e}")
            total_failed += 1

    print("\nBatch processing complete")
    print(f"Processed videos: {total_processed}")
    print(f"Failed videos: {total_failed}")
    print(f"Total extracted keyframes: {total_extracted}")
    print(f"Total label-ready keyframes: {total_labeling}")
    if not args.keep_pruned_images:
        print(f"Total deleted pruned images: {total_deleted}")


if __name__ == "__main__":
    main()