"""
run_parsers.py
--------------
Step 2 orchestrator: runs all 5 specialist parsers in parallel.

Architecture:
  - Reads detections.json produced by yolo_detector.py (Step 1).
  - Groups detections by class_name.
  - Dispatches each class group to its specialist parser.
  - Uses ProcessPoolExecutor for true parallelism across CPU cores.
  - Each parser worker is called via a top-level function (required for pickling).
  - Writes one output JSON per class to data/visual_outputs/.
  - Writes a combined all_parsed_objects.json for downstream steps.

Parallelism strategy:
  We use ProcessPoolExecutor (not ThreadPoolExecutor) because:
  - Nougat, PaddleOCR, and torch inference release the GIL inconsistently.
  - Separate processes guarantee true parallelism and isolate crashes.
  - Each process loads its model ONCE (lazy singleton pattern) and reuses it
    across all tasks assigned to that worker (via initializer pattern).

  Each class runs in a dedicated process pool so GPU memory is not shared.
  On a machine with 1 GPU: parsers run sequentially by class but detections
  within each class run in parallel threads (I/O bound).
  On multi-GPU: set MAX_WORKERS_PER_CLASS > 1 and use CUDA_VISIBLE_DEVICES.

Usage:
  python run_parsers.py \
      --detections data/detections/all_detections.json \
      --output_dir data/visual_outputs \
      --workers 4

  # Run only specific classes (useful for re-runs after failure):
  python run_parsers.py \
      --detections data/detections/all_detections.json \
      --output_dir data/visual_outputs \
      --classes equation graph
"""

import json
import os
import sys
import argparse
import time
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Optional

# Add src/layer2 to path so workers can import parsers
sys.path.insert(0, str(Path(__file__).parent))

from src.diagram_parsers.base_parser import ParsedObject

# ── Class → parser module + class name mapping ────────────────────────────────

CLASS_PARSER_MAP = {
    "equation": ("equation_parser", "EquationParser"),
    "graph":    ("graph_parser",    "GraphParser"),
    "diagram":  ("diagram_parser",  "DiagramParser"),
    "table":    ("table_parser",    "TableParser"),
    "text":     ("text_parser",     "TextParser"),
}

# ── Worker entry point (must be module-level for pickling) ────────────────────

def _worker_parse(args: tuple) -> dict:
    """
    Called in a subprocess for each detection.
    Imports parser lazily — models load once per worker process.
    Returns serializable dict.
    """
    module_name, class_name, detection = args
    import importlib
    module = importlib.import_module(module_name)
    parser_class = getattr(module, class_name)
    parser = parser_class()
    result: ParsedObject = parser.parse(detection)
    return result.to_dict()


# ── Per-class parallel runner ─────────────────────────────────────────────────

def _run_class_parallel(
    class_name: str,
    detections: list[dict],
    output_path: str,
    max_workers: int,
) -> list[dict]:
    """
    Run one class of detections through its parser using a process pool.
    Writes results to output_path and returns list of result dicts.
    """
    module_name, parser_class_name = CLASS_PARSER_MAP[class_name]
    total = len(detections)
    results = []
    failed = 0

    print(f"\n[{class_name.upper()}] Starting {total} detections "
          f"with {max_workers} worker(s)...")
    t0 = time.time()

    args_list = [(module_name, parser_class_name, det) for det in detections]

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_worker_parse, args): args[2]["detection_id"]
                   for args in args_list}

        for i, future in enumerate(as_completed(futures), 1):
            detection_id = futures[future]
            try:
                result_dict = future.result()
                results.append(result_dict)
                status = result_dict.get("extraction_status", "?")
                elapsed = time.time() - t0
                avg_sec = elapsed / i
                eta = avg_sec * (total - i)
                print(f"  [{class_name}] {i}/{total} — {detection_id} "
                      f"[{status}] | avg {avg_sec:.1f}s | ETA {eta:.0f}s")
                if status == "failed":
                    failed += 1
            except Exception as exc:
                failed += 1
                print(f"  [{class_name}] EXCEPTION on {detection_id}: {exc}")
                # Still record a failed entry so we don't lose track of it
                results.append({
                    "detection_id": detection_id,
                    "class_name": class_name,
                    "extraction_status": "failed",
                    "error_message": str(exc),
                    "text_representation": f"{class_name}: worker exception",
                })

    elapsed_total = time.time() - t0
    success = total - failed
    print(f"\n[{class_name.upper()}] Done. "
          f"{success}/{total} succeeded, {failed} failed "
          f"in {elapsed_total:.1f}s ({elapsed_total/max(total,1):.1f}s/item)")

    # Write class-specific output
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"[{class_name.upper()}] Saved → {output_path}")

    return results


# ── Summary helpers ───────────────────────────────────────────────────────────

def _print_summary(all_results: list[dict]) -> None:
    from collections import Counter
    status_counts = Counter(r.get("extraction_status", "unknown") for r in all_results)
    class_counts  = Counter(r.get("class_name", "unknown") for r in all_results)

    print("\n" + "=" * 60)
    print("PARSER SUMMARY")
    print("=" * 60)
    print(f"Total objects processed: {len(all_results)}")
    print("\nBy class:")
    for cls, count in sorted(class_counts.items()):
        print(f"  {cls:<12} {count:>4}")
    print("\nBy status:")
    for status, count in sorted(status_counts.items()):
        print(f"  {status:<20} {count:>4}")

    failed = [r for r in all_results if r.get("extraction_status") == "failed"]
    if failed:
        print(f"\nFailed detections ({len(failed)}) — review manually:")
        for r in failed[:10]:
            print(f"  {r['detection_id']} — {r.get('error_message','')[:80]}")
        if len(failed) > 10:
            print(f"  ... and {len(failed) - 10} more")
    print("=" * 60)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Run specialist visual parsers in parallel.")
    parser.add_argument(
        "--detections",
        default="data/detections/all_detections.json",
        help="Path to all_detections.json from yolo_detector.py",
    )
    parser.add_argument(
        "--output_dir",
        default="data/visual_outputs",
        help="Directory to write per-class output JSON files",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=2,
        help=(
            "Number of parallel worker processes per class. "
            "Set to 1 if GPU memory is limited. "
            "VLM parsers (diagram) should use 1 on single-GPU machines."
        ),
    )
    parser.add_argument(
        "--classes",
        nargs="+",
        choices=list(CLASS_PARSER_MAP.keys()),
        default=list(CLASS_PARSER_MAP.keys()),
        help="Which classes to process. Defaults to all 5.",
    )
    parser.add_argument(
        "--skip_existing",
        action="store_true",
        help="Skip a class if its output JSON already exists (useful for re-runs).",
    )
    args = parser.parse_args()

    # ── Load detections ──────────────────────────────────────────────────────
    print(f"Loading detections from: {args.detections}")
    with open(args.detections) as f:
        all_detections = json.load(f)

    # Filter out frames with no detections
    valid = [d for d in all_detections if d.get("class_name")]
    print(f"Total valid detections: {len(valid)} "
          f"(skipped {len(all_detections) - len(valid)} empty frames)")

    # Group by class
    by_class: dict[str, list[dict]] = {cls: [] for cls in CLASS_PARSER_MAP}
    for det in valid:
        cls = det.get("class_name")
        if cls in by_class:
            by_class[cls].append(det)

    print("\nDetections per class:")
    for cls, dets in by_class.items():
        print(f"  {cls:<12} {len(dets):>4}")

    # ── Run parsers sequentially by class ────────────────────────────────────
    # Classes run one at a time to avoid GPU OOM; within each class,
    # detections run in parallel across `--workers` processes.
    #
    # To run classes in parallel (multi-GPU setup), wrap _run_class_parallel
    # calls in a ThreadPoolExecutor here and set CUDA_VISIBLE_DEVICES per class.

    os.makedirs(args.output_dir, exist_ok=True)
    all_results = []
    pipeline_start = time.time()

    for class_name in args.classes:
        detections = by_class.get(class_name, [])
        if not detections:
            print(f"\n[{class_name.upper()}] No detections — skipping.")
            continue

        output_path = os.path.join(args.output_dir, f"{class_name}_outputs.json")

        if args.skip_existing and os.path.exists(output_path):
            print(f"\n[{class_name.upper()}] Output exists, loading from disk...")
            with open(output_path) as f:
                class_results = json.load(f)
            all_results.extend(class_results)
            continue

        # GPU-heavy parsers should use 1 worker to avoid OOM
        workers = args.workers
        if class_name in ("diagram",):
            workers = 1
            print(f"[{class_name.upper()}] Forcing workers=1 (VLM, GPU memory constraint)")

        class_results = _run_class_parallel(
            class_name=class_name,
            detections=detections,
            output_path=output_path,
            max_workers=workers,
        )
        all_results.extend(class_results)

    # ── Write combined output ─────────────────────────────────────────────────
    combined_path = os.path.join(args.output_dir, "all_parsed_objects.json")
    with open(combined_path, "w") as f:
        json.dump(all_results, f, indent=2)
    print(f"\nCombined output saved → {combined_path}")

    total_elapsed = time.time() - pipeline_start
    print(f"Total pipeline time: {total_elapsed:.1f}s ({total_elapsed/60:.1f} min)")

    _print_summary(all_results)


if __name__ == "__main__":
    main()
