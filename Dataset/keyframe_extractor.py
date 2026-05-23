#!/usr/bin/env python3
"""
Adaptive keyframe extraction for educational and general videos.
Auto-calibrates thresholds per video and avoids hardcoded channel presets.
"""

import argparse
import json
from collections import deque
from pathlib import Path

import cv2
import numpy as np
from skimage.metrics import structural_similarity as ssim


class SmartWhiteboardExtractor:
    """
    Adaptive extractor that calibrates itself per-video.
    Works across whiteboard, slide-based, and mixed educational content.
    """

    def __init__(
        self,
        motion_threshold=5.0,
        stability_frames=24,
        min_interval=90,
        content_threshold=0.01,
        frame_skip=5,
        analysis_scale=0.25,
        auto_calibrate=True,
        warmup_samples=80,
        aggressiveness=0.35,
        prune_for_labeling=True,
        prune_unique_ratio=0.03,
        prune_similarity_threshold=0.92,
    ):
        self.motion_threshold = motion_threshold
        self.stability_frames = max(3, int(stability_frames))
        self.min_interval = max(1, int(min_interval))
        self.content_threshold = content_threshold
        self.frame_skip = max(1, int(frame_skip))
        self.analysis_scale = float(analysis_scale)
        self.auto_calibrate = auto_calibrate
        self.warmup_samples = max(20, int(warmup_samples))
        self.aggressiveness = float(np.clip(aggressiveness, 0.0, 1.0))
        self.prune_for_labeling = bool(prune_for_labeling)
        self.prune_unique_ratio = float(np.clip(prune_unique_ratio, 0.005, 0.30))
        self.prune_similarity_threshold = float(np.clip(prune_similarity_threshold, 0.75, 0.99))

        self.ssim_threshold = 0.90
        self.phash_threshold = 6
        self.roi_change_threshold = 0.04

        self.motion_history = deque(maxlen=self.stability_frames)
        self.last_capture_frame = -self.min_interval
        self.last_stable_frame = None
        self.last_hash = None
        self.stable_streak = 0
        self.adjustment_log = []
        self.profile = None
        self.target_fpm_min = 0.4
        self.target_fpm_max = 8.0

    def process_video(self, video_path, output_dir):
        """Extract complete and non-redundant keyframes from a video."""
        video_path = Path(video_path)
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        cap = cv2.VideoCapture(str(video_path))
        if not cap.isOpened():
            raise RuntimeError(f"Could not open video: {video_path}")

        fps = cap.get(cv2.CAP_PROP_FPS)
        if fps <= 0:
            fps = 30.0
        total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        duration = total_frames / max(fps, 1e-6)
        cap.release()

        if self.auto_calibrate:
            self.profile = self._profile_video(video_path, fps, total_frames)
            self._apply_profile(self.profile, fps)
        else:
            self.profile = {
                "mode": "manual",
                "derived": {
                    "motion_threshold": self.motion_threshold,
                    "stability_frames": self.stability_frames,
                    "min_interval": self.min_interval,
                    "content_threshold": self.content_threshold,
                    "frame_skip": self.frame_skip,
                },
            }

        self.motion_history = deque(maxlen=self.stability_frames)
        self.last_capture_frame = -self.min_interval
        self.last_stable_frame = None
        self.last_hash = None
        self.stable_streak = 0

        effective_fps = fps / self.frame_skip

        print(f"\n{'=' * 70}")
        print("Adaptive Keyframe Extraction")
        print(f"{'=' * 70}")
        print(f"Video: {video_path.name}")
        print(f"Duration: {duration / 60:.1f} minutes ({total_frames} frames at {fps:.1f} fps)")
        print("Calibrated settings:")
        print(f"  - Motion threshold: {self.motion_threshold:.3f}")
        print(f"  - Stability period: {self.stability_frames / max(effective_fps, 1e-6):.2f}s")
        print(f"  - Min interval: {self.min_interval / max(fps, 1e-6):.2f}s")
        print(f"  - Frame skip: every {self.frame_skip} frame(s) ({effective_fps:.1f} effective fps)")
        print(f"  - Content threshold: {self.content_threshold:.4f}")
        print(f"  - Aggressiveness: {self.aggressiveness:.2f}")
        if self.profile and "style" in self.profile:
            print(f"  - Inferred style: {self.profile['style']}")
            derived = self.profile.get("derived", {})
            if "target_fpm_min" in derived and "target_fpm_max" in derived:
                print(
                    "  - Target density band: "
                    f"{derived['target_fpm_min']:.2f}-{derived['target_fpm_max']:.2f} frames/min"
                )
        print(f"{'=' * 70}\n")

        cap = cv2.VideoCapture(str(video_path))
        keyframes = []
        prev_frame = None
        frame_idx = 0
        processed_idx = 0
        progress_step = max(total_frames // 10, 1)
        guardrail_step = max(int(fps * 60), 1)

        while True:
            for _ in range(self.frame_skip - 1):
                if not cap.grab():
                    break
                frame_idx += 1

            ret, frame = cap.read()
            if not ret:
                break

            gray_full = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
            gray = cv2.resize(
                gray_full,
                None,
                fx=self.analysis_scale,
                fy=self.analysis_scale,
                interpolation=cv2.INTER_AREA,
            )

            if prev_frame is not None:
                motion_score, motion_components = self._calculate_motion(prev_frame, gray)
                self.motion_history.append(motion_score)

                stability_score = self._stability_score()
                if stability_score >= 0.7:
                    self.stable_streak += 1
                else:
                    self.stable_streak = 0

                can_capture = self._can_capture(frame_idx)
                content_score, content_components = self._content_score(gray)
                is_different, diff_components = self._difference_score(gray)

                min_stable_streak = max(2, self.stability_frames // 6)
                should_capture = (
                    can_capture
                    and self.stable_streak >= min_stable_streak
                    and stability_score >= 0.7
                    and content_score >= 0.95
                    and is_different
                )

                if should_capture:
                    timestamp = frame_idx / max(fps, 1e-6)
                    keyframe = {
                        "frame": frame_idx,
                        "timestamp": timestamp,
                        "motion_avg": float(np.mean(self.motion_history)),
                        "method": "adaptive_complete_content",
                        "scores": {
                            "stability": float(stability_score),
                            "content": float(content_score),
                            "motion": float(motion_score),
                        },
                        "components": {
                            "motion": motion_components,
                            "content": content_components,
                            "difference": diff_components,
                        },
                    }
                    keyframes.append(keyframe)

                    filename = f"complete_{len(keyframes):03d}_f{frame_idx:06d}_t{timestamp:.2f}.jpg"
                    filepath = output_dir / filename
                    cv2.imwrite(str(filepath), frame)
                    keyframe["filename"] = filename

                    self.last_capture_frame = frame_idx
                    self.last_stable_frame = gray.copy()
                    self.last_hash = self._phash(gray)
                    self.stable_streak = 0

                    print(
                        f"  + [{len(keyframes):3d}] {timestamp:7.2f}s "
                        f"(stable: {stability_score:.2f}, content: {content_score:.2f}, motion: {motion_score:.2f})"
                    )

            prev_frame = gray
            frame_idx += 1
            processed_idx += 1

            if frame_idx % progress_step == 0:
                progress = (frame_idx / max(total_frames, 1)) * 100
                print(f"      Progress: {progress:5.1f}% ({len(keyframes)} frames captured)")

            if frame_idx > 0 and frame_idx % guardrail_step == 0:
                self._apply_guardrails(
                    frame_idx=frame_idx,
                    keyframes_count=len(keyframes),
                    fps=fps,
                    total_frames=total_frames,
                )

        cap.release()

        if (
            prev_frame is not None
            and self._difference_score(prev_frame)[0]
            and self._content_score(prev_frame)[0] >= 0.95
            and self._can_capture(frame_idx - 1)
        ):
            cap2 = cv2.VideoCapture(str(video_path))
            cap2.set(cv2.CAP_PROP_POS_FRAMES, max(0, frame_idx - 1))
            ret, last_frame = cap2.read()
            cap2.release()

            if ret:
                timestamp = (frame_idx - 1) / max(fps, 1e-6)
                keyframe = {
                    "frame": frame_idx - 1,
                    "timestamp": timestamp,
                    "motion_avg": 0.0,
                    "method": "last_frame",
                    "scores": {"stability": 1.0, "content": float(self._content_score(prev_frame)[0]), "motion": 0.0},
                    "components": {},
                }
                keyframes.append(keyframe)

                filename = f"complete_{len(keyframes):03d}_f{frame_idx - 1:06d}_t{timestamp:.2f}.jpg"
                filepath = output_dir / filename
                cv2.imwrite(str(filepath), last_frame)
                keyframe["filename"] = filename

                self.last_stable_frame = prev_frame

        metadata = {
            "video_path": str(video_path),
            "duration": duration,
            "fps": fps,
            "total_frames": total_frames,
            "keyframes_extracted": len(keyframes),
            "avg_interval": duration / max(len(keyframes), 1),
            "settings": {
                "motion_threshold": self.motion_threshold,
                "stability_frames": self.stability_frames,
                "min_interval": self.min_interval,
                "content_threshold": self.content_threshold,
                "frame_skip": self.frame_skip,
                "analysis_scale": self.analysis_scale,
                "ssim_threshold": self.ssim_threshold,
                "phash_threshold": self.phash_threshold,
                "roi_change_threshold": self.roi_change_threshold,
                "prune_for_labeling": self.prune_for_labeling,
                "prune_unique_ratio": self.prune_unique_ratio,
                "prune_similarity_threshold": self.prune_similarity_threshold,
            },
            "profile": self.profile,
            "adaptive_adjustments": self.adjustment_log,
            "keyframes": keyframes,
        }

        keep_indices, prune_report = self._select_labeling_keyframes(keyframes, output_dir)
        metadata["labeling"] = prune_report
        metadata["keyframes_for_labeling"] = [keyframes[i] for i in keep_indices]

        metadata_path = output_dir / "extraction_metadata.json"
        with open(metadata_path, "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=2)

        print(f"\n{'=' * 70}")
        print("Extraction Complete")
        print(f"   Frames extracted: {len(keyframes)}")
        print(
            "   Frames for labeling: "
            f"{len(metadata['keyframes_for_labeling'])} "
            f"(pruned {len(keyframes) - len(metadata['keyframes_for_labeling'])})"
        )
        print(f"   Average interval: {metadata['avg_interval']:.1f}s")
        print(f"   Frames per minute: {len(keyframes) / max(duration / 60, 1e-6):.1f}")
        print(f"   Metadata saved: {metadata_path}")
        print(f"{'=' * 70}\n")

        return keyframes

    def _profile_video(self, video_path, fps, total_frames):
        """Warm-up pass that estimates robust video-level statistics."""
        cap = cv2.VideoCapture(str(video_path))
        sample_count = int(min(self.warmup_samples, max(total_frames - 1, 1)))
        sample_indices = np.linspace(0, max(total_frames - 1, 0), num=sample_count, dtype=np.int64)

        brightness = []
        contrast = []
        saturation = []
        edge_density = []
        detail_density = []
        motion_scores = []
        similarity_scores = []

        prev_gray = None
        for idx in sample_indices:
            cap.set(cv2.CAP_PROP_POS_FRAMES, int(idx))
            ret, frame = cap.read()
            if not ret:
                continue

            gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
            gray = cv2.resize(
                gray,
                None,
                fx=self.analysis_scale,
                fy=self.analysis_scale,
                interpolation=cv2.INTER_AREA,
            )

            hsv = cv2.cvtColor(frame, cv2.COLOR_BGR2HSV)
            sat = hsv[:, :, 1]

            edges = cv2.Canny(gray, 50, 150)
            lap_var = cv2.Laplacian(gray, cv2.CV_64F).var()

            brightness.append(float(np.mean(gray)))
            contrast.append(float(np.std(gray)))
            saturation.append(float(np.mean(sat)))
            edge_density.append(float(np.count_nonzero(edges) / edges.size))
            detail_density.append(float(lap_var))

            if prev_gray is not None:
                motion_score, _ = self._calculate_motion(prev_gray, gray)
                motion_scores.append(float(motion_score))
                try:
                    similarity_scores.append(float(ssim(prev_gray, gray)))
                except Exception:
                    similarity_scores.append(1.0)

            prev_gray = gray

        cap.release()

        def pct(values, q, fallback=0.0):
            return float(np.percentile(values, q)) if values else fallback

        motion_p30 = pct(motion_scores, 30, 1.0)
        motion_p80 = pct(motion_scores, 80, motion_p30 + 1.0)
        motion_variability = float(np.std(motion_scores) / (np.mean(motion_scores) + 1e-6)) if motion_scores else 0.0
        edge_p40 = pct(edge_density, 40, 0.01)
        sat_mean = float(np.mean(saturation)) if saturation else 30.0
        sim_p20 = pct(similarity_scores, 20, 0.95)

        if sat_mean < 30 and edge_p40 > 0.008:
            style = "board_like"
        elif sim_p20 < 0.75:
            style = "slide_like"
        else:
            style = "mixed"

        effective_target_fps = 12.0
        frame_skip = max(1, int(round(fps / effective_target_fps)))
        effective_fps = fps / frame_skip

        capture_bias = float(np.clip(self.aggressiveness, 0.0, 1.0))

        motion_threshold = motion_p30 + 0.30 * (motion_p80 - motion_p30)
        motion_threshold *= 0.85 + 0.50 * capture_bias

        stability_seconds = np.clip(1.0 + 1.4 / (1.0 + motion_variability), 0.8, 2.8)
        min_interval_seconds = np.clip(1.6 + 2.3 / (1.0 + motion_variability), 1.3, 5.5)

        # Aggressiveness is now strongly coupled to capture density:
        # lower values wait longer and require stronger uniqueness.
        stability_seconds *= 1.35 - 0.70 * capture_bias
        min_interval_seconds *= 1.45 - 0.90 * capture_bias

        if style == "slide_like":
            min_interval_seconds *= 1.20
            stability_seconds *= 0.85
        elif style == "board_like":
            min_interval_seconds *= 0.90
            stability_seconds *= 1.10

        stability_frames = max(3, int(round(stability_seconds * max(effective_fps, 1.0))))
        min_interval = max(int(round(min_interval_seconds * fps)), stability_frames)
        content_threshold = float(np.clip(edge_p40 * (1.10 - 0.55 * capture_bias), 0.004, 0.09))

        ssim_base = 0.90 if style != "slide_like" else 0.94
        phash_base = 6 if style != "slide_like" else 4
        roi_base = 0.04 if style != "slide_like" else 0.025

        ssim_threshold = float(np.clip(ssim_base + (capture_bias - 0.5) * 0.06, 0.84, 0.97))
        phash_threshold = int(np.clip(round(phash_base + (0.5 - capture_bias) * 4), 3, 12))
        roi_change_threshold = float(np.clip(roi_base + (0.5 - capture_bias) * 0.03, 0.015, 0.10))

        style_base_rate = {
            "board_like": 3.0,
            "slide_like": 1.5,
            "mixed": 2.2,
        }.get(style, 2.0)
        target_rate = style_base_rate * (0.55 + 0.90 * capture_bias)
        target_fpm_min = max(0.25, target_rate * 0.5)
        target_fpm_max = max(target_fpm_min + 0.5, target_rate * 1.8)

        return {
            "mode": "auto",
            "style": style,
            "warmup": {
                "sample_count": sample_count,
                "motion_p30": motion_p30,
                "motion_p80": motion_p80,
                "motion_variability": motion_variability,
                "edge_density_p40": edge_p40,
                "saturation_mean": sat_mean,
                "similarity_p20": sim_p20,
                "brightness_mean": float(np.mean(brightness)) if brightness else 0.0,
                "contrast_mean": float(np.mean(contrast)) if contrast else 0.0,
                "detail_mean": float(np.mean(detail_density)) if detail_density else 0.0,
            },
            "derived": {
                "motion_threshold": float(motion_threshold),
                "stability_frames": int(stability_frames),
                "min_interval": int(min_interval),
                "content_threshold": float(content_threshold),
                "frame_skip": int(frame_skip),
                "analysis_scale": float(self.analysis_scale),
                "ssim_threshold": float(ssim_threshold),
                "phash_threshold": int(phash_threshold),
                "roi_change_threshold": float(roi_change_threshold),
                "target_fpm_min": float(target_fpm_min),
                "target_fpm_max": float(target_fpm_max),
            },
        }

    def _apply_profile(self, profile, fps):
        derived = profile.get("derived", {})
        self.motion_threshold = float(derived.get("motion_threshold", self.motion_threshold))
        self.stability_frames = max(3, int(derived.get("stability_frames", self.stability_frames)))
        self.min_interval = max(1, int(derived.get("min_interval", self.min_interval)))
        self.content_threshold = float(derived.get("content_threshold", self.content_threshold))
        self.frame_skip = max(1, int(derived.get("frame_skip", self.frame_skip)))
        self.analysis_scale = float(derived.get("analysis_scale", self.analysis_scale))
        self.ssim_threshold = float(derived.get("ssim_threshold", self.ssim_threshold))
        self.phash_threshold = int(derived.get("phash_threshold", self.phash_threshold))
        self.roi_change_threshold = float(derived.get("roi_change_threshold", self.roi_change_threshold))
        self.target_fpm_min = float(derived.get("target_fpm_min", self.target_fpm_min))
        self.target_fpm_max = float(derived.get("target_fpm_max", self.target_fpm_max))

        # Keep min interval at least one processed frame interval in real frame units.
        self.min_interval = max(self.min_interval, int(round(fps / max(self.frame_skip, 1))))

    def _calculate_motion(self, prev_frame, curr_frame):
        """Fused motion score: abs diff, optical flow, and changed area."""
        diff = cv2.absdiff(prev_frame, curr_frame)
        diff_mean = float(np.mean(diff))
        changed_ratio = float(np.count_nonzero(diff > 12) / diff.size)

        flow_mag_mean = 0.0
        try:
            flow = cv2.calcOpticalFlowFarneback(
                prev_frame,
                curr_frame,
                None,
                0.5,
                3,
                15,
                3,
                5,
                1.2,
                0,
            )
            mag, _ = cv2.cartToPolar(flow[..., 0], flow[..., 1])
            flow_mag_mean = float(np.mean(mag))
        except Exception:
            flow_mag_mean = 0.0

        score = 0.55 * diff_mean + 0.35 * (flow_mag_mean * 25.0) + 0.10 * (changed_ratio * 255.0)
        components = {
            "diff_mean": diff_mean,
            "flow_mag_mean": flow_mag_mean,
            "changed_ratio": changed_ratio,
        }
        return float(score), components

    def _stability_score(self):
        """Continuous stability score in [0, 1] from recent motion history."""
        if len(self.motion_history) < self.stability_frames:
            return 0.0

        recent_motion = np.asarray(self.motion_history, dtype=np.float32)
        avg_motion = float(np.mean(recent_motion))
        max_motion = float(np.max(recent_motion))

        avg_norm = np.clip(avg_motion / max(self.motion_threshold, 1e-6), 0.0, 2.0)
        max_norm = np.clip(max_motion / max(self.motion_threshold * 1.5, 1e-6), 0.0, 2.0)
        score = 1.0 - (0.65 * avg_norm + 0.35 * max_norm)
        return float(np.clip(score, 0.0, 1.0))

    def _can_capture(self, current_frame):
        return (current_frame - self.last_capture_frame) >= self.min_interval

    def _content_score(self, frame):
        """Multi-cue content score based on edges, adaptive foreground, and detail."""
        edges = cv2.Canny(frame, 50, 150)
        edge_ratio = float(np.count_nonzero(edges) / edges.size)

        adapt = cv2.adaptiveThreshold(
            frame,
            255,
            cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
            cv2.THRESH_BINARY_INV,
            21,
            7,
        )
        foreground_ratio = float(np.count_nonzero(adapt) / adapt.size)

        lap_var = float(cv2.Laplacian(frame, cv2.CV_64F).var())
        lap_norm = float(np.clip(lap_var / 150.0, 0.0, 1.5))
        edge_norm = float(np.clip(edge_ratio / max(self.content_threshold, 1e-6), 0.0, 2.0))
        fg_norm = float(np.clip(foreground_ratio / max(self.content_threshold * 1.25, 1e-6), 0.0, 2.0))

        score = 0.45 * edge_norm + 0.35 * fg_norm + 0.20 * lap_norm
        components = {
            "edge_ratio": edge_ratio,
            "foreground_ratio": foreground_ratio,
            "laplacian_var": lap_var,
        }
        return float(score), components

    def _difference_score(self, current_frame):
        """Hybrid duplicate detection using SSIM, pHash, and ROI change."""
        if self.last_stable_frame is None:
            return True, {"ssim": None, "phash_distance": None, "roi_change": None}

        try:
            similarity = float(ssim(self.last_stable_frame, current_frame))
        except Exception:
            similarity = 0.0

        current_hash = self._phash(current_frame)
        if self.last_hash is None:
            self.last_hash = self._phash(self.last_stable_frame)
        phash_distance = self._hash_distance(self.last_hash, current_hash)

        roi_change = self._roi_change(self.last_stable_frame, current_frame)
        is_different = (
            similarity < self.ssim_threshold
            or phash_distance >= self.phash_threshold
            or roi_change > self.roi_change_threshold
        )

        return is_different, {
            "ssim": similarity,
            "phash_distance": int(phash_distance),
            "roi_change": float(roi_change),
        }

    def _phash(self, frame):
        small = cv2.resize(frame, (32, 32), interpolation=cv2.INTER_AREA).astype(np.float32)
        dct = cv2.dct(small)
        dct_low = dct[:8, :8]
        med = float(np.median(dct_low[1:, 1:]))
        return (dct_low > med).astype(np.uint8).flatten()

    def _hash_distance(self, h1, h2):
        return int(np.count_nonzero(h1 != h2))

    def _roi_change(self, prev_frame, curr_frame):
        h, w = prev_frame.shape[:2]
        y0 = h // 5
        y1 = h - h // 5
        x0 = w // 6
        x1 = w - w // 6
        prev_roi = prev_frame[y0:y1, x0:x1]
        curr_roi = curr_frame[y0:y1, x0:x1]
        if prev_roi.size == 0 or curr_roi.size == 0:
            return 0.0
        diff = cv2.absdiff(prev_roi, curr_roi)
        return float(np.count_nonzero(diff > 10) / diff.size)

    def _apply_guardrails(self, frame_idx, keyframes_count, fps, total_frames):
        elapsed_min = frame_idx / max(fps * 60.0, 1e-6)
        if elapsed_min <= 0.5:
            return

        current_rate = keyframes_count / elapsed_min
        changed = False
        note = {
            "frame": int(frame_idx),
            "elapsed_min": float(elapsed_min),
            "current_rate": float(current_rate),
        }

        if current_rate < self.target_fpm_min:
            self.motion_threshold *= 1.10
            self.content_threshold *= 0.90
            self.min_interval = max(1, int(self.min_interval * 0.90))
            note["action"] = "relax"
            changed = True
        elif current_rate > self.target_fpm_max:
            self.motion_threshold *= 0.88
            self.content_threshold *= 1.12
            self.min_interval = int(self.min_interval * 1.20)
            note["action"] = "tighten"
            changed = True

        if changed:
            note["updated"] = {
                "motion_threshold": float(self.motion_threshold),
                "content_threshold": float(self.content_threshold),
                "min_interval": int(self.min_interval),
                "target_fpm_min": float(self.target_fpm_min),
                "target_fpm_max": float(self.target_fpm_max),
            }
            self.adjustment_log.append(note)

    def _select_labeling_keyframes(self, keyframes, output_dir):
        """Prune temporal supersets so labeling sees fewer redundant frames."""
        if not keyframes:
            return [], {
                "enabled": self.prune_for_labeling,
                "input_count": 0,
                "output_count": 0,
                "pruned_count": 0,
                "method": "none",
            }

        if not self.prune_for_labeling or len(keyframes) <= 2:
            keep = list(range(len(keyframes)))
            return keep, {
                "enabled": self.prune_for_labeling,
                "input_count": len(keyframes),
                "output_count": len(keep),
                "pruned_count": 0,
                "method": "passthrough",
                "reason": "disabled_or_too_few_frames",
            }

        gray_frames = []
        masks = []
        valid_indices = []
        for idx, kf in enumerate(keyframes):
            filepath = output_dir / kf["filename"]
            img = cv2.imread(str(filepath), cv2.IMREAD_GRAYSCALE)
            if img is None:
                continue
            scaled = cv2.resize(
                img,
                None,
                fx=self.analysis_scale,
                fy=self.analysis_scale,
                interpolation=cv2.INTER_AREA,
            )
            gray_frames.append(scaled)
            masks.append(self._content_mask_binary(scaled))
            valid_indices.append(idx)

        if len(valid_indices) <= 2:
            keep = valid_indices if valid_indices else list(range(len(keyframes)))
            return keep, {
                "enabled": True,
                "input_count": len(keyframes),
                "output_count": len(keep),
                "pruned_count": len(keyframes) - len(keep),
                "method": "fallback",
                "reason": "insufficient_valid_images",
            }

        keep_local = []
        future_union = np.zeros_like(masks[0], dtype=np.uint8)
        diagnostics = []

        for local_idx in range(len(valid_indices) - 1, -1, -1):
            mask = masks[local_idx]
            gray = gray_frames[local_idx]
            pixel_count = int(np.count_nonzero(mask))

            if local_idx == len(valid_indices) - 1:
                keep_local.append(local_idx)
                future_union = cv2.bitwise_or(future_union, mask)
                diagnostics.append({
                    "frame": int(keyframes[valid_indices[local_idx]]["frame"]),
                    "decision": "keep_last",
                    "unique_ratio": 1.0,
                    "similarity_to_next_kept": None,
                })
                continue

            if pixel_count == 0:
                unique_ratio = 0.0
            else:
                new_pixels = cv2.bitwise_and(mask, cv2.bitwise_not(future_union))
                unique_ratio = float(np.count_nonzero(new_pixels) / max(pixel_count, 1))

            next_kept_local = keep_local[-1]
            try:
                sim_next = float(ssim(gray, gray_frames[next_kept_local]))
            except Exception:
                sim_next = 0.0

            should_keep = (
                unique_ratio >= self.prune_unique_ratio
                or sim_next < self.prune_similarity_threshold
            )

            if local_idx == 0:
                should_keep = True

            diagnostics.append({
                "frame": int(keyframes[valid_indices[local_idx]]["frame"]),
                "decision": "keep" if should_keep else "drop",
                "unique_ratio": round(unique_ratio, 4),
                "similarity_to_next_kept": round(sim_next, 4),
            })

            if should_keep:
                keep_local.append(local_idx)
                future_union = cv2.bitwise_or(future_union, mask)

        keep_local_sorted = sorted(keep_local)
        keep_indices = [valid_indices[i] for i in keep_local_sorted]
        keep_set = set(keep_indices)

        for i in range(len(keyframes)):
            if i not in valid_indices:
                keep_set.add(i)

        keep_final = sorted(keep_set)
        return keep_final, {
            "enabled": True,
            "input_count": len(keyframes),
            "output_count": len(keep_final),
            "pruned_count": len(keyframes) - len(keep_final),
            "method": "backward_content_union",
            "unique_ratio_threshold": self.prune_unique_ratio,
            "similarity_threshold": self.prune_similarity_threshold,
            "diagnostics": diagnostics,
        }

    def _content_mask_binary(self, gray_frame):
        """Build a stable binary content mask for redundancy pruning."""
        adapt = cv2.adaptiveThreshold(
            gray_frame,
            255,
            cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
            cv2.THRESH_BINARY_INV,
            21,
            7,
        )
        edges = cv2.Canny(gray_frame, 50, 150)
        mask = cv2.bitwise_or(adapt, edges)
        kernel = np.ones((3, 3), np.uint8)
        mask = cv2.morphologyEx(mask, cv2.MORPH_OPEN, kernel)
        mask = cv2.morphologyEx(mask, cv2.MORPH_CLOSE, kernel)
        return mask


def validate_extraction(metadata_path):
    """Validate extracted frames and report density and score diagnostics."""
    with open(metadata_path, "r", encoding="utf-8") as f:
        metadata = json.load(f)

    keyframes = metadata.get("keyframes", [])
    duration = float(metadata.get("duration", 0.0))

    print(f"\n{'=' * 70}")
    print("Validation Report")
    print(f"{'=' * 70}\n")

    issues = []
    warnings = []

    frames_per_minute = len(keyframes) / max(duration / 60.0, 1e-6)
    if frames_per_minute < 0.5:
        warnings.append(f"Very few frames: {frames_per_minute:.1f} per minute")
    elif frames_per_minute > 8:
        warnings.append(f"Many frames: {frames_per_minute:.1f} per minute (possible redundancy)")
    else:
        print(f"+ Frame density: {frames_per_minute:.1f} per minute")

    if len(keyframes) > 1:
        intervals = [
            keyframes[i]["timestamp"] - keyframes[i - 1]["timestamp"]
            for i in range(1, len(keyframes))
        ]
        avg_interval = float(np.mean(intervals))
        std_interval = float(np.std(intervals))
        print(f"+ Average interval: {avg_interval:.1f}s (+/- {std_interval:.1f}s)")

        max_gap = float(np.max(intervals))
        if max_gap > 180:
            warnings.append(f"Long gap detected: {max_gap:.0f}s")

    content_scores = [kf.get("scores", {}).get("content") for kf in keyframes]
    content_scores = [v for v in content_scores if isinstance(v, (int, float))]
    if content_scores:
        mean_content = float(np.mean(content_scores))
        print(f"+ Mean content score: {mean_content:.2f}")
        if mean_content < 1.0:
            warnings.append("Average content score is low; consider lower frame skip or higher analysis scale")

    labeling_info = metadata.get("labeling", {})
    if labeling_info:
        out_count = int(labeling_info.get("output_count", len(keyframes)))
        pruned = int(labeling_info.get("pruned_count", 0))
        print(f"+ Label-ready frames: {out_count} (pruned: {pruned})")

    if warnings:
        print("\nWarnings:")
        for warning in warnings:
            print(f"  - {warning}")

    if issues:
        print("\nIssues:")
        for issue in issues:
            print(f"  - {issue}")

    if not warnings and not issues:
        print("\nAll checks passed")

    print(f"\n{'=' * 70}\n")
    return issues, warnings


def main():
    parser = argparse.ArgumentParser(description="Adaptive Keyframe Extraction")
    parser.add_argument("--video", required=True, help="Path to input video")
    parser.add_argument("--output", required=True, help="Output directory for keyframes")
    parser.add_argument("--validate", action="store_true", help="Validate extraction after completion")
    parser.add_argument(
        "--no-auto-calibrate",
        action="store_true",
        help="Disable auto-calibration and use only manual values/defaults",
    )
    parser.add_argument(
        "--warmup-samples",
        type=int,
        default=80,
        help="Number of sampled frames for auto-calibration",
    )
    parser.add_argument(
        "--aggressiveness",
        type=float,
        default=0.35,
        help="Capture aggressiveness in [0,1], lower means fewer captures",
    )

    parser.add_argument("--motion-threshold", type=float, help="Manual motion threshold override")
    parser.add_argument("--stability-frames", type=int, help="Manual stability frames override")
    parser.add_argument("--min-interval", type=int, help="Manual min interval override (in source frames)")
    parser.add_argument("--frame-skip", type=int, help="Manual frame skip override")
    parser.add_argument("--analysis-scale", type=float, help="Manual analysis scale override")
    parser.add_argument("--content-threshold", type=float, help="Manual content threshold override")
    parser.add_argument(
        "--no-prune-for-labeling",
        action="store_true",
        help="Disable post-extraction pruning before labeling",
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

    args = parser.parse_args()

    params = {
        "auto_calibrate": not args.no_auto_calibrate,
        "warmup_samples": args.warmup_samples,
        "aggressiveness": args.aggressiveness,
        "prune_for_labeling": not args.no_prune_for_labeling,
        "prune_unique_ratio": args.prune_unique_ratio,
        "prune_similarity_threshold": args.prune_similarity_threshold,
    }

    if args.motion_threshold is not None:
        params["motion_threshold"] = args.motion_threshold
    if args.stability_frames is not None:
        params["stability_frames"] = args.stability_frames
    if args.min_interval is not None:
        params["min_interval"] = args.min_interval
    if args.frame_skip is not None:
        params["frame_skip"] = args.frame_skip
    if args.analysis_scale is not None:
        params["analysis_scale"] = args.analysis_scale
    if args.content_threshold is not None:
        params["content_threshold"] = args.content_threshold

    extractor = SmartWhiteboardExtractor(**params)
    extractor.process_video(args.video, args.output)

    if args.validate:
        metadata_path = Path(args.output) / "extraction_metadata.json"
        validate_extraction(metadata_path)


if __name__ == "__main__":
    main()
