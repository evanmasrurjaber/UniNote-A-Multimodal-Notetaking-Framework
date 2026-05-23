import os
import cv2
import numpy as np
import argparse
import json
from tqdm import tqdm

class MITOCWExtractorRaw:
    def __init__(self, output_dir="data/keyframes/"):
        """
        Initializes the Pure Mathematical Keyframe Extractor.
        """
        self.output_dir = output_dir
        
        # --- Mathematical Hyperparameters ---
        self.sample_rate_fps = 1          # Process only 1 frame per second to speed up extraction
        self.downsample_res = (640, 360)  # Downsample to 360p for ultra-fast array math
        self.motion_threshold = 15.0      # Pixel diff threshold to detect human/camera movement
        self.min_stable_seconds = 5       # The board must remain completely unchanged for 5 seconds
        
    def process_video(self, video_path):
        """
        Processes a single video using the steady-state trigger.
        """
        video_name = os.path.splitext(os.path.basename(video_path))[0]
        save_dir = os.path.join(self.output_dir, video_name)
        os.makedirs(save_dir, exist_ok=True)
        
        cap = cv2.VideoCapture(video_path)
        if not cap.isOpened():
            print(f"❌ Cannot open video: {video_path}")
            return

        fps = cap.get(cv2.CAP_PROP_FPS)
        total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        
        # Guard against metadata errors in downloaded files
        if fps <= 0 or total_frames <= 0:
            print(f"❌ Invalid video metadata for {video_name}. Skipping.")
            return
            
        frame_skip = int(fps / self.sample_rate_fps)
        
        print(f"\n🎬 Processing: {video_name} ({total_frames} frames @ {fps:.2f} FPS)")
        
        prev_gray = None
        stable_count = 0
        last_stable_frame = None
        last_stable_timestamp = 0
        extracted_count = 0
        metadata = []

        # Initialize progress bar
        with tqdm(total=total_frames, unit="frames", desc="Extracting", bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]") as pbar:
            
            frame_idx = 0
            while cap.isOpened():
                ret, frame = cap.read()
                if not ret:
                    break
                    
                # Update progress bar
                pbar.update(1)
                
                # Tier 1: Temporal Skip
                if frame_idx % frame_skip != 0:
                    frame_idx += 1
                    continue
                    
                timestamp = frame_idx / fps
                
                # Tier 1: Spatial Downsample & Grayscale (for fast math only, we save the high-res frame later)
                resized = cv2.resize(frame, self.downsample_res)
                gray = cv2.cvtColor(resized, cv2.COLOR_BGR2GRAY)
                
                if prev_gray is None:
                    prev_gray = gray
                    frame_idx += 1
                    continue
                    
                # Tier 2: Steady-State Mathematical Trigger
                # Calculate mean absolute pixel difference
                diff = np.mean(cv2.absdiff(gray, prev_gray))
                
                if diff < self.motion_threshold:
                    # The frame is mathematically stable (professor stepped aside)
                    stable_count += 1
                    last_stable_frame = frame  # Keep the high-resolution BGR frame in memory
                    last_stable_timestamp = timestamp
                else:
                    # Motion detected! (Professor walked in front of the board or erased it)
                    # Did we just exit a long, uninterrupted stable period?
                    if stable_count >= self.min_stable_seconds and last_stable_frame is not None:
                        
                        # Save the frame captured right before the motion happened
                        out_path = os.path.join(save_dir, f"frame_{int(last_stable_timestamp):04d}s.jpg")
                        cv2.imwrite(out_path, last_stable_frame)
                        extracted_count += 1
                        
                        metadata.append({
                            "timestamp_seconds": round(last_stable_timestamp, 2),
                            "filename": os.path.basename(out_path)
                        })
                            
                    # Reset the stability tracker now that the board is moving again
                    stable_count = 0
                    last_stable_frame = None

                prev_gray = gray
                frame_idx += 1
                
            # Cleanup: Check if the video ended while perfectly stable
            if stable_count >= self.min_stable_seconds and last_stable_frame is not None:
                out_path = os.path.join(save_dir, f"frame_{int(last_stable_timestamp):04d}s.jpg")
                cv2.imwrite(out_path, last_stable_frame)
                extracted_count += 1
                metadata.append({
                    "timestamp_seconds": round(last_stable_timestamp, 2),
                    "filename": os.path.basename(out_path)
                })

        cap.release()
        
        # Save metadata JSON for the dataset registry
        with open(os.path.join(save_dir, "extraction_metadata.json"), "w") as f:
            json.dump({"video": video_name, "keyframes_extracted": extracted_count, "keyframes": metadata}, f, indent=4)
            
        print(f"✅ Finished {video_name}. Extracted {extracted_count} raw keyframes.")

def main():
    parser = argparse.ArgumentParser(description="Pure Math Keyframe Extractor for MIT OCW")
    
    parser.add_argument('--video', type=str, help='Path to a single video file')
    parser.add_argument('--bulk_dir', type=str, help='Path to a directory containing video files')
    parser.add_argument('--output', type=str, default='data/keyframes/', help='Output directory for keyframes')
    
    args = parser.parse_args()
    
    extractor = MITOCWExtractorRaw(output_dir=args.output)
    
    if args.video:
        if os.path.exists(args.video):
            extractor.process_video(args.video)
        else:
            print(f"❌ Video not found: {args.video}")
            
    elif args.bulk_dir:
        if os.path.exists(args.bulk_dir):
            # Target standard video extensions
            video_extensions = ('.mp4', '.mkv', '.avi', '.webm')
            video_files = [os.path.join(args.bulk_dir, f) for f in os.listdir(args.bulk_dir) if f.lower().endswith(video_extensions)]
            
            print(f"🔍 Found {len(video_files)} videos for bulk processing.")
            for v_file in video_files:
                extractor.process_video(v_file)
        else:
            print(f"❌ Directory not found: {args.bulk_dir}")
            
    else:
        print("⚠️ Please provide either --video <path> or --bulk_dir <path>")

if __name__ == "__main__":
    main()