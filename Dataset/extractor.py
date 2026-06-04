import os
import cv2
import numpy as np
import argparse
import json
from tqdm import tqdm

class GeneralKeyframeExtractor:
    def __init__(self, output_dir="data/keyframes/"):
        """
        Initializes a General-Purpose Keyframe Extractor optimized for 
        Slide Presentations, Digital Tablet Writing, and Screen Shares.
        """
        self.output_dir = output_dir
        
        # --- Simplified Digital Hyperparameters ---
        self.sample_rate_fps = 1          # 1 FPS is plenty for lectures
        self.downsample_res = (640, 360)  # Downsample for ultra-fast array math
        
        # The threshold for how much the screen must change to trigger a new capture.
        # (A full slide change might be a diff of 30+. A new bullet point might be 5.0).
        self.content_shift_threshold = 8.0 
        
        # Wait 2 seconds after capturing a frame before looking for the next one.
        # This prevents capturing blurry frames mid-way through a slide transition animation.
        self.cooldown_seconds = 2.0       
        
    def process_video(self, video_path):
        video_name = os.path.splitext(os.path.basename(video_path))[0]
        save_dir = os.path.join(self.output_dir, video_name)
        os.makedirs(save_dir, exist_ok=True)
        
        cap = cv2.VideoCapture(video_path)
        if not cap.isOpened():
            print(f"❌ Cannot open video: {video_path}")
            return

        fps = cap.get(cv2.CAP_PROP_FPS)
        total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        
        if fps <= 0 or total_frames <= 0:
            print(f"❌ Invalid video metadata for {video_name}. Skipping.")
            return
            
        frame_skip = int(fps / self.sample_rate_fps)
        
        print(f"\n🎬 Processing: {video_name} ({total_frames} frames @ {fps:.2f} FPS)")
        
        last_saved_gray = None
        last_saved_timestamp = -self.cooldown_seconds
        extracted_count = 0
        metadata = []

        with tqdm(total=total_frames, unit="frames", desc="Extracting", bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]") as pbar:
            
            frame_idx = 0
            while cap.isOpened():
                ret, frame = cap.read()
                if not ret:
                    break
                    
                pbar.update(1)
                
                # 1. Temporal Subsampling
                if frame_idx % frame_skip != 0:
                    frame_idx += 1
                    continue
                    
                timestamp = frame_idx / fps
                
                # 2. Cooldown Guard (Ignores mid-transition animations)
                if (timestamp - last_saved_timestamp) < self.cooldown_seconds:
                    frame_idx += 1
                    continue
                
                # 3. Spatial Downsampling for math
                resized = cv2.resize(frame, self.downsample_res)
                gray = cv2.cvtColor(resized, cv2.COLOR_BGR2GRAY)
                
                # 4. First Frame Capture
                if last_saved_gray is None:
                    last_saved_gray = gray
                    last_saved_timestamp = timestamp
                    out_path = os.path.join(save_dir, f"frame_{int(timestamp):04d}s.jpg")
                    cv2.imwrite(out_path, frame)
                    extracted_count += 1
                    metadata.append({"timestamp_seconds": round(timestamp, 2), "filename": os.path.basename(out_path)})
                    frame_idx += 1
                    continue
                    
                # 5. The Cumulative State-Change Trigger
                # We compare the current screen to the LAST SAVED screen, not the previous second.
                diff = np.mean(cv2.absdiff(gray, last_saved_gray))
                
                # If enough new digital ink or a new slide has appeared:
                if diff > self.content_shift_threshold:
                    out_path = os.path.join(save_dir, f"frame_{int(timestamp):04d}s.jpg")
                    cv2.imwrite(out_path, frame)
                    
                    last_saved_gray = gray  # Reset our baseline to this new slide/state
                    last_saved_timestamp = timestamp
                    extracted_count += 1
                    
                    metadata.append({
                        "timestamp_seconds": round(timestamp, 2),
                        "filename": os.path.basename(out_path)
                    })

                frame_idx += 1

        cap.release()
        
        with open(os.path.join(save_dir, "extraction_metadata.json"), "w") as f:
            json.dump({"video": video_name, "keyframes_extracted": extracted_count, "keyframes": metadata}, f, indent=4)
            
        print(f"✅ Finished {video_name}. Extracted {extracted_count} keyframes.")

def main():
    parser = argparse.ArgumentParser(description="General Keyframe Extractor for Slides & Digital Lectures")
    parser.add_argument('--video', type=str, help='Path to a single video file')
    parser.add_argument('--bulk_dir', type=str, help='Path to a directory containing video files')
    parser.add_argument('--output', type=str, default='data/keyframes/', help='Output directory for keyframes')
    args = parser.parse_args()
    
    extractor = GeneralKeyframeExtractor(output_dir=args.output)
    
    if args.video and os.path.exists(args.video):
        extractor.process_video(args.video)
    elif args.bulk_dir and os.path.exists(args.bulk_dir):
        video_extensions = ('.mp4', '.mkv', '.avi', '.webm')
        video_files = [os.path.join(args.bulk_dir, f) for f in os.listdir(args.bulk_dir) if f.lower().endswith(video_extensions)]
        print(f"🔍 Found {len(video_files)} videos for bulk processing.")
        for v_file in video_files:
            extractor.process_video(v_file)
    else:
        print("⚠️ Please provide a valid --video <path> or --bulk_dir <path>")

if __name__ == "__main__":
    main()