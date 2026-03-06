#!/usr/bin/env python3
"""
Smart Keyframe Extraction for Whiteboard Educational Videos
Specifically designed for Khan Academy / Organic Chemistry Tutor style videos
Detects when content is COMPLETE (not half-written)
"""

import cv2
import numpy as np
from pathlib import Path
import json
from collections import deque
from skimage.metrics import structural_similarity as ssim
import argparse

class SmartWhiteboardExtractor:
    """
    Extracts frames only when content is complete
    Avoids half-written equations, partial diagrams, empty frames
    """
    
    def __init__(self,
                 motion_threshold=5.0,
                 stability_frames=60,      # 2 seconds at 30fps
                 min_interval=90,          # 3 seconds between captures
                 content_threshold=0.01,
                 frame_skip=5,             # process every Nth frame (1=no skip)
                 analysis_scale=0.25):     # downscale factor for analysis

        self.motion_threshold = motion_threshold
        self.stability_frames = stability_frames
        self.min_interval = min_interval
        self.content_threshold = content_threshold
        self.frame_skip = max(1, frame_skip)
        self.analysis_scale = analysis_scale
        
        self.motion_history = deque(maxlen=stability_frames)
        self.last_capture_frame = -min_interval
        self.last_stable_frame = None
    
    def process_video(self, video_path, output_dir):
        """
        Extract frames with complete content from video
        """
        
        video_path = Path(video_path)
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        cap = cv2.VideoCapture(str(video_path))
        fps = cap.get(cv2.CAP_PROP_FPS)
        total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        duration = total_frames / fps
        
        effective_fps = fps / self.frame_skip

        print(f"\n{'='*70}")
        print(f"Smart Whiteboard Extraction")
        print(f"{'='*70}")
        print(f"Video: {video_path.name}")
        print(f"Duration: {duration/60:.1f} minutes ({total_frames} frames at {fps:.1f} fps)")
        print(f"Settings:")
        print(f"  - Motion threshold: {self.motion_threshold}")
        print(f"  - Stability period: {self.stability_frames/effective_fps:.1f}s")
        print(f"  - Min interval: {self.min_interval/effective_fps:.1f}s")
        print(f"  - Frame skip: every {self.frame_skip} frame(s) ({effective_fps:.1f} effective fps)")
        print(f"  - Analysis scale: {self.analysis_scale}")
        print(f"{'='*70}\n")

        keyframes = []
        prev_frame = None
        frame_idx = 0
        progress_step = max(total_frames // 10, 1)

        while True:
            # Skip frames cheaply using grab() (no decode)
            for _ in range(self.frame_skip - 1):
                if not cap.grab():
                    break
                frame_idx += 1

            ret, frame = cap.read()
            if not ret:
                break

            # Convert to grayscale at full resolution (for saving later)
            gray_full = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)

            # Downscale for analysis — all detection runs on this
            gray = cv2.resize(gray_full, None,
                              fx=self.analysis_scale, fy=self.analysis_scale,
                              interpolation=cv2.INTER_AREA)

            if prev_frame is not None:
                # Calculate motion between frames (on downscaled)
                motion_score = self._calculate_motion(prev_frame, gray)
                self.motion_history.append(motion_score)

                # Lazy evaluation: cheap checks first, expensive ones only if needed
                is_stable = self._is_content_stable()
                if is_stable:
                    can_capture = self._can_capture(frame_idx)
                else:
                    can_capture = False

                if is_stable and can_capture:
                    has_content = self._has_meaningful_content(gray)
                else:
                    has_content = False

                if is_stable and can_capture and has_content:
                    is_different = self._is_content_different(gray)
                else:
                    is_different = False

                should_capture = is_stable and can_capture and has_content and is_different

                if should_capture:
                    timestamp = frame_idx / fps

                    keyframe = {
                        'frame': frame_idx,
                        'timestamp': timestamp,
                        'motion_avg': float(np.mean(self.motion_history)),
                        'method': 'complete_content'
                    }

                    keyframes.append(keyframe)

                    # Save full-resolution frame
                    filename = f"complete_{len(keyframes):03d}_f{frame_idx:06d}_t{timestamp:.2f}.jpg"
                    filepath = output_dir / filename
                    cv2.imwrite(str(filepath), frame)

                    keyframe['filename'] = filename

                    # Update tracking (store downscaled version for SSIM comparison)
                    self.last_capture_frame = frame_idx
                    self.last_stable_frame = gray.copy()

                    print(f"  ✓ [{len(keyframes):3d}] {timestamp:7.2f}s  "
                          f"(motion: {keyframe['motion_avg']:.2f})")

            # Swap reference instead of copying — prev_frame just points to
            # the current array, which won't be mutated before next iteration.
            prev_frame = gray
            frame_idx += 1

            # Progress indicator (every 10%)
            if frame_idx % progress_step == 0:
                progress = (frame_idx / total_frames) * 100
                print(f"      Progress: {progress:5.1f}% ({len(keyframes)} frames captured)")
        
        cap.release()

        # Force-capture the last frame if it's different from the last saved one
        # prev_frame is already downscaled at this point
        if prev_frame is not None and self._is_content_different(prev_frame) and self._has_meaningful_content(prev_frame):
            # Re-open video to grab the last frame in full resolution
            cap2 = cv2.VideoCapture(str(video_path))
            cap2.set(cv2.CAP_PROP_POS_FRAMES, frame_idx - 1)
            ret, last_frame = cap2.read()
            cap2.release()

            if ret:
                timestamp = (frame_idx - 1) / fps
                keyframe = {
                    'frame': frame_idx - 1,
                    'timestamp': timestamp,
                    'motion_avg': 0.0,
                    'method': 'last_frame'
                }
                keyframes.append(keyframe)

                filename = f"complete_{len(keyframes):03d}_f{frame_idx - 1:06d}_t{timestamp:.2f}.jpg"
                filepath = output_dir / filename
                cv2.imwrite(str(filepath), last_frame)
                keyframe['filename'] = filename

                self.last_stable_frame = prev_frame

        # Save metadata
        metadata = {
            'video_path': str(video_path),
            'duration': duration,
            'fps': fps,
            'total_frames': total_frames,
            'keyframes_extracted': len(keyframes),
            'avg_interval': duration / max(len(keyframes), 1),
            'settings': {
                'motion_threshold': self.motion_threshold,
                'stability_frames': self.stability_frames,
                'min_interval': self.min_interval
            },
            'keyframes': keyframes
        }
        
        metadata_path = output_dir / 'extraction_metadata.json'
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        print(f"\n{'='*70}")
        print(f"✅ Extraction Complete")
        print(f"   Frames extracted: {len(keyframes)}")
        print(f"   Average interval: {metadata['avg_interval']:.1f}s")
        print(f"   Frames per minute: {len(keyframes)/(duration/60):.1f}")
        print(f"   Metadata saved: {metadata_path}")
        print(f"{'='*70}\n")
        
        return keyframes
    
    def _calculate_motion(self, prev_frame, curr_frame):
        """
        Calculate motion score between consecutive frames
        Higher score = more writing/drawing activity
        """
        # Method 1: Simple absolute difference
        diff = cv2.absdiff(prev_frame, curr_frame)
        motion_score = np.mean(diff)
        
        return motion_score
    
    def _is_content_stable(self):
        """
        Check if content has been stable (no writing) for required period
        Returns True if writing has stopped and content is complete
        """
        if len(self.motion_history) < self.stability_frames:
            return False
        
        # Check if all recent frames have low motion
        recent_motion = list(self.motion_history)
        avg_motion = np.mean(recent_motion)
        max_motion = np.max(recent_motion)
        
        # Content is stable if:
        # 1. Average motion is below threshold
        # 2. No spikes in motion (no writing happening)
        is_stable = (
            avg_motion < self.motion_threshold and 
            max_motion < self.motion_threshold * 1.5
        )
        
        return is_stable
    
    def _can_capture(self, current_frame):
        """
        Check if enough time has passed since last capture
        Prevents capturing duplicate/redundant content
        """
        return (current_frame - self.last_capture_frame) >= self.min_interval
    
    def _has_meaningful_content(self, frame):
        """
        Check if frame has substantial content (not empty board)
        Uses edge detection to find written/drawn content
        """
        # Detect edges (written content)
        edges = cv2.Canny(frame, 50, 150)
        content_ratio = np.count_nonzero(edges) / edges.size
        
        # Only capture if there's substantial content
        return content_ratio > self.content_threshold
    
    def _is_content_different(self, current_frame):
        """
        Check if current frame is different from last captured frame
        Prevents capturing the same content multiple times
        """
        if self.last_stable_frame is None:
            return True
        
        # Calculate structural similarity
        try:
            similarity = ssim(self.last_stable_frame, current_frame)
            
            # Different if similarity is below 95%
            return similarity < 0.90
        except:
            # If SSIM fails, assume it's different
            return True
    

def validate_extraction(metadata_path):
    """
    Validate extracted frames for quality issues
    """
    with open(metadata_path, 'r') as f:
        metadata = json.load(f)
    
    keyframes = metadata['keyframes']
    duration = metadata['duration']
    
    print(f"\n{'='*70}")
    print(f"Validation Report")
    print(f"{'='*70}\n")
    
    issues = []
    warnings = []
    
    # Check 1: Reasonable frame density
    frames_per_minute = len(keyframes) / (duration / 60)
    
    if frames_per_minute < 0.5:
        warnings.append(f"Very few frames: {frames_per_minute:.1f} per minute")
    elif frames_per_minute > 8:
        warnings.append(f"Many frames: {frames_per_minute:.1f} per minute (may have redundancy)")
    else:
        print(f"✓ Frame density: {frames_per_minute:.1f} per minute (good)")
    
    # Check 2: Interval consistency
    if len(keyframes) > 1:
        intervals = []
        for i in range(1, len(keyframes)):
            interval = keyframes[i]['timestamp'] - keyframes[i-1]['timestamp']
            intervals.append(interval)
        
        avg_interval = np.mean(intervals)
        std_interval = np.std(intervals)
        
        print(f"✓ Average interval: {avg_interval:.1f}s (±{std_interval:.1f}s)")
        
        # Check for very long gaps
        max_gap = np.max(intervals)
        if max_gap > 180:  # 3 minutes
            warnings.append(f"Long gap detected: {max_gap:.0f}s")
    
    # Print warnings
    if warnings:
        print(f"\n⚠️  Warnings:")
        for warning in warnings:
            print(f"   - {warning}")
    
    if issues:
        print(f"\n❌ Issues:")
        for issue in issues:
            print(f"   - {issue}")
    
    if not warnings and not issues:
        print(f"\n✅ All checks passed!")
    
    print(f"\n{'='*70}\n")
    
    return issues, warnings


# Presets for different video styles
PRESETS = {
    'khan_academy': {
        'motion_threshold': 4.0,
        'stability_frames': 90,   # 3 seconds
        'min_interval': 120,      # 4 seconds
        'content_threshold': 0.01,
        'frame_skip': 5,
        'analysis_scale': 0.25
    },
    'organic_chem_tutor': {
        'motion_threshold': 6.0,
        'stability_frames': 45,   # 1.5 seconds
        'min_interval': 60,       # 2 seconds
        'content_threshold': 0.01,
        'frame_skip': 5,
        'analysis_scale': 0.25
    },
    'default': {
        'motion_threshold': 5.0,
        'stability_frames': 60,   # 2 seconds
        'min_interval': 90,       # 3 seconds
        'content_threshold': 0.01,
        'frame_skip': 5,
        'analysis_scale': 0.25
    }
}


def main():
    parser = argparse.ArgumentParser(
        description='Smart Keyframe Extraction for Whiteboard Videos'
    )
    parser.add_argument('--video', required=True, 
                       help='Path to input video')
    parser.add_argument('--output', required=True,
                       help='Output directory for keyframes')
    parser.add_argument('--preset', choices=['khan_academy', 'organic_chem_tutor', 'default'],
                       default='default',
                       help='Preset for video style')
    parser.add_argument('--validate', action='store_true',
                       help='Validate extraction after completion')
    
    # Manual parameter overrides
    parser.add_argument('--motion-threshold', type=float,
                       help='Motion threshold (override preset)')
    parser.add_argument('--stability-frames', type=int,
                       help='Stability frames (override preset)')
    parser.add_argument('--min-interval', type=int,
                       help='Min interval between captures (override preset)')
    parser.add_argument('--frame-skip', type=int,
                       help='Process every Nth frame (default: 5)')
    parser.add_argument('--analysis-scale', type=float,
                       help='Downscale factor for analysis, e.g. 0.25 (override preset)')
    
    args = parser.parse_args()
    
    # Get preset parameters
    params = PRESETS[args.preset].copy()
    
    # Override with manual parameters if provided
    if args.motion_threshold:
        params['motion_threshold'] = args.motion_threshold
    if args.stability_frames:
        params['stability_frames'] = args.stability_frames
    if args.min_interval:
        params['min_interval'] = args.min_interval
    if args.frame_skip:
        params['frame_skip'] = args.frame_skip
    if args.analysis_scale:
        params['analysis_scale'] = args.analysis_scale
    
    # Create extractor
    extractor = SmartWhiteboardExtractor(**params)
    
    # Process video
    keyframes = extractor.process_video(args.video, args.output)
    
    # Validate if requested
    if args.validate:
        metadata_path = Path(args.output) / 'extraction_metadata.json'
        validate_extraction(metadata_path)


if __name__ == '__main__':
    main()
