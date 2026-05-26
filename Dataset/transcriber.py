import os
import whisper
import subprocess
import argparse
import time
from pathlib import Path

class OCWTranscriber:
    def __init__(self, model_size='small', device='cuda'):
        self.model_size = model_size
        self.device = device
        self.model = None

    def load_model(self):
        """Lazy load the model only when transcription starts."""
        if self.model is None:
            print(f"🧠 Loading Whisper '{self.model_size}' model on {self.device}...")
            self.model = whisper.load_model(self.model_size, device=self.device)

    def _format_timestamp(self, seconds):
        """Format as VTT timestamp"""
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = seconds % 60
        return f"{hours:02d}:{minutes:02d}:{secs:06.3f}"

    def process_video(self, video_path_str):
        video_path = Path(video_path_str)
        if not video_path.exists():
            print(f"❌ File not found: {video_path}")
            return

        # Dynamically map the folders based on the video's location
        # e.g., data/raw_dataset/6.006_Introduction_to_Algorithms/
        course_dir = video_path.parent.parent 
        transcript_dir = course_dir / 'transcripts'
        audio_dir = course_dir / 'audio_temp'

        transcript_dir.mkdir(parents=True, exist_ok=True)
        audio_dir.mkdir(parents=True, exist_ok=True)

        base_name = video_path.stem
        audio_path = audio_dir / f"{base_name}.wav"
        txt_path = transcript_dir / f"{base_name}.txt"
        vtt_path = transcript_dir / f"{base_name}.vtt"

        if txt_path.exists() and vtt_path.exists():
            print(f"⏩ Transcripts already exist for {base_name}. Skipping.")
            return

        print(f"\n🎬 Processing: {base_name}")

        # Step 1: Extract Audio (Using your exact 16kHz Mono FFmpeg logic)
        if not audio_path.exists():
            print("   → Extracting 16kHz mono audio via FFmpeg...")
            cmd = [
                'ffmpeg', '-i', str(video_path),
                '-vn', '-ar', '16000', '-ac', '1', '-c:a', 'pcm_s16le',
                str(audio_path), '-y', '-loglevel', 'error'
            ]
            try:
                subprocess.run(cmd, check=True)
            except Exception as e:
                print(f"   ❌ FFmpeg extraction failed: {e}")
                return

        # Step 2: Transcribe
        self.load_model()
        print("   → Transcribing audio with Whisper...")
        start_time = time.time()
        try:
            result = self.model.transcribe(
                str(audio_path),
                language='en',
                task='transcribe',
                verbose=False,
                # Added 'computer science' and 'algorithms' to your excellent prompt
                initial_prompt="This is an educational video about science, mathematics, computer science, algorithms, physics, chemistry, or biology."
            )
        except Exception as e:
            print(f"   ❌ Transcription failed: {e}")
            return
            
        elapsed = time.time() - start_time
        word_count = len(result['text'].split())
        print(f"   ✓ Transcribed {word_count} words in {elapsed:.1f}s")

        # Step 3: Save Plain Text
        with open(txt_path, 'w', encoding='utf-8') as f:
            f.write(result['text'].strip())

        # Step 4: Save WebVTT (Using your exact VTT logic)
        with open(vtt_path, 'w', encoding='utf-8') as f:
            f.write("WEBVTT\n\n")
            for segment in result['segments']:
                start = self._format_timestamp(segment['start'])
                end = self._format_timestamp(segment['end'])
                text = segment['text'].strip()
                f.write(f"{start} --> {end}\n{text}\n\n")

        print(f"   ✓ Saved to: {txt_path.name} & {vtt_path.name}")

        # Cleanup: Delete the temporary 1GB+ .wav file to save hard drive space
        audio_path.unlink(missing_ok=True)

def main():
    parser = argparse.ArgumentParser(description="Whisper Transcriber for MIT OCW")
    parser.add_argument('--model', default='small', choices=['tiny', 'base', 'small', 'medium', 'large'], help='Whisper model size')
    parser.add_argument('--device', default='cuda', choices=['cuda', 'cpu'], help='Processing device')
    
    parser.add_argument('--video', type=str, help='Path to a single video file')
    parser.add_argument('--bulk_dir', type=str, help='Path to the videos directory')
    
    args = parser.parse_args()
    transcriber = OCWTranscriber(model_size=args.model, device=args.device)
    
    if args.video:
        transcriber.process_video(args.video)
    elif args.bulk_dir:
        video_dir = Path(args.bulk_dir)
        if video_dir.exists() and video_dir.is_dir():
            videos = [f for f in video_dir.iterdir() if f.suffix.lower() in ['.mp4', '.mkv', '.webm']]
            print(f"🔍 Found {len(videos)} videos to transcribe.")
            for vid in videos:
                transcriber.process_video(str(vid))
        else:
            print(f"❌ Directory not found: {args.bulk_dir}")
    else:
        print("⚠️ Provide either --video <path> or --bulk_dir <path>")

if __name__ == "__main__":
    main()