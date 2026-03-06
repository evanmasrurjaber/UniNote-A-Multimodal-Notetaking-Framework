#!/usr/bin/env python3
"""
Complete Transcription Pipeline
Generates transcripts for videos without YouTube subtitles using Whisper
"""

import whisper
import json
from pathlib import Path
import subprocess
import time
import argparse
import sys

class CompleteTranscriptionPipeline:
    def __init__(self, 
                 data_dir='data/raw_videos',
                 model_size='small',
                 device='cuda'):
        
        self.data_dir = Path(data_dir)
        self.video_dir = self.data_dir / 'videos'
        self.transcript_dir = self.data_dir / 'transcripts'
        self.audio_dir = Path('data/audio_temp')
        self.metadata_file = self.data_dir / 'collection_log.json'
        
        # Create directories
        self.transcript_dir.mkdir(parents=True, exist_ok=True)
        self.audio_dir.mkdir(parents=True, exist_ok=True)
        
        # Load Whisper model
        self.model_size = model_size
        self.device = device
        self.model = None
    
    def step1_identify_missing(self):
        """Step 1: Identify videos needing transcription"""
        
        print(f"\n{'='*70}")
        print("STEP 1: IDENTIFYING VIDEOS NEEDING TRANSCRIPTION")
        print(f"{'='*70}\n")
        
        # Load metadata
        with open(self.metadata_file, 'r') as f:
            metadata = json.load(f)
        
        missing = []
        
        for video in metadata['videos']:
            video_id = video['video_id']
            video_index = video['video_index']
            
            # Check for transcript
            txt_file = self.transcript_dir / f"{video_index:03d}_{video_id}_transcript.txt"
            
            if not txt_file.exists() or video.get('needs_whisper_transcription', False):
                missing.append(video)
        
        print(f"Total videos: {len(metadata['videos'])}")
        print(f"Need transcription: {len(missing)}")
        print(f"Percentage: {len(missing)/len(metadata['videos'])*100:.1f}%\n")
        
        if missing:
            print("First 10 videos needing transcription:")
            for i, vid in enumerate(missing[:10], 1):
                duration_min = vid['duration'] / 60
                print(f"  {i:2d}. [{vid['video_index']:03d}] {vid['title'][:45]:45s} ({duration_min:4.1f}min)")
            
            if len(missing) > 10:
                print(f"\n  ... and {len(missing)-10} more")
        
        return missing
    
    def step2_extract_audio(self, videos_list):
        """Step 2: Extract audio from videos"""
        
        print(f"\n{'='*70}")
        print("STEP 2: EXTRACTING AUDIO")
        print(f"{'='*70}\n")
        
        audio_files = []
        
        for i, video in enumerate(videos_list, 1):
            print(f"[{i}/{len(videos_list)}] {video['title'][:50]}")
            
            video_path = self.video_dir / video['filename']
            audio_path = self.audio_dir / f"{video['video_id']}.wav"
            
            # Skip if already extracted
            if audio_path.exists():
                print(f"  → Audio already exists")
                audio_files.append({
                    'video_id': video['video_id'],
                    'video_index': video['video_index'],
                    'audio_path': str(audio_path),
                    'duration': video['duration']
                })
                continue
            
            # Extract audio
            cmd = [
                'ffmpeg',
                '-i', str(video_path),
                '-vn',              # No video
                '-ar', '16000',     # 16kHz (Whisper optimal)
                '-ac', '1',         # Mono
                '-c:a', 'pcm_s16le',
                str(audio_path),
                '-y',               # Overwrite
                '-loglevel', 'error'
            ]
            
            try:
                subprocess.run(cmd, check=True, timeout=600)
                print(f"  ✓ Audio extracted")
                
                audio_files.append({
                    'video_id': video['video_id'],
                    'video_index': video['video_index'],
                    'audio_path': str(audio_path),
                    'duration': video['duration']
                })
                
            except Exception as e:
                print(f"  ✗ Error: {e}")
        
        print(f"\n✅ Extracted audio from {len(audio_files)}/{len(videos_list)} videos\n")
        
        return audio_files
    
    def step3_transcribe(self, audio_files):
        """Step 3: Transcribe using Whisper"""
        
        print(f"\n{'='*70}")
        print("STEP 3: TRANSCRIBING WITH WHISPER")
        print(f"{'='*70}\n")
        
        # Load model if not loaded
        if self.model is None:
            print(f"Loading Whisper model: {self.model_size}")
            print("This may take a few minutes...")
            self.model = whisper.load_model(self.model_size, device=self.device)
            print(f"✓ Model loaded\n")
        
        results = []
        total_duration = 0
        total_processing = 0
        
        for i, audio_info in enumerate(audio_files, 1):
            print(f"[{i}/{len(audio_files)}] Transcribing...")
            
            audio_path = Path(audio_info['audio_path'])
            video_id = audio_info['video_id']
            video_index = audio_info['video_index']
            
            # Check if already transcribed
            txt_file = self.transcript_dir / f"{video_index:03d}_{video_id}_transcript.txt"
            if txt_file.exists():
                print(f"  → Already transcribed")
                continue
            
            start_time = time.time()
            
            try:
                # Transcribe
                result = self.model.transcribe(
                    str(audio_path),
                    language='en',
                    task='transcribe',
                    verbose=False,
                    initial_prompt="This is an educational video about science, mathematics, physics, chemistry, or biology."
                )
                
                # Save TXT
                with open(txt_file, 'w', encoding='utf-8') as f:
                    f.write(result['text'])
                
                # Save VTT
                vtt_file = self.transcript_dir / f"{video_index:03d}_{video_id}_transcript.vtt"
                self._save_vtt(result, vtt_file)
                
                elapsed = time.time() - start_time
                word_count = len(result['text'].split())
                
                print(f"  ✓ Transcribed in {elapsed:.1f}s ({word_count} words)")
                print(f"    Speed: {audio_info['duration']/elapsed:.1f}x realtime")
                
                total_duration += audio_info['duration']
                total_processing += elapsed
                
                results.append({
                    'video_id': video_id,
                    'video_index': video_index,
                    'success': True
                })
                
            except Exception as e:
                print(f"  ✗ Error: {e}")
                results.append({
                    'video_id': video_id,
                    'video_index': video_index,
                    'success': False,
                    'error': str(e)
                })
        
        # Summary
        if results:
            successful = sum(1 for r in results if r['success'])
            avg_speed = total_duration / total_processing if total_processing > 0 else 0
            
            print(f"\n{'='*70}")
            print("TRANSCRIPTION COMPLETE")
            print(f"{'='*70}")
            print(f"Successful: {successful}/{len(results)}")
            print(f"Total duration: {total_duration/60:.1f} minutes")
            print(f"Total processing: {total_processing/60:.1f} minutes")
            print(f"Average speed: {avg_speed:.1f}x realtime")
            print(f"{'='*70}\n")
        
        return results
    
    def _save_vtt(self, result, output_file):
        """Save VTT format"""
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write("WEBVTT\n\n")
            
            for segment in result['segments']:
                start = self._format_timestamp(segment['start'])
                end = self._format_timestamp(segment['end'])
                text = segment['text'].strip()
                
                f.write(f"{start} --> {end}\n")
                f.write(f"{text}\n\n")
    
    def _format_timestamp(self, seconds):
        """Format as VTT timestamp"""
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = seconds % 60
        return f"{hours:02d}:{minutes:02d}:{secs:06.3f}"
    
    def step4_validate(self, videos_list):
        """Step 4: Validate transcripts"""
        
        print(f"\n{'='*70}")
        print("STEP 4: VALIDATING TRANSCRIPTS")
        print(f"{'='*70}\n")
        
        results = []
        
        for video in videos_list:
            video_id = video['video_id']
            video_index = video['video_index']
            
            txt_file = self.transcript_dir / f"{video_index:03d}_{video_id}_transcript.txt"
            
            print(f"[{video_index:03d}] {video['title'][:45]:45s} ... ", end='')
            
            if not txt_file.exists():
                print("✗ Missing")
                results.append({'video_id': video_id, 'passed': False})
                continue
            
            # Read and check
            with open(txt_file, 'r', encoding='utf-8') as f:
                text = f.read()
            
            word_count = len(text.split())
            
            if word_count < 100:
                print(f"✗ Too short ({word_count} words)")
                results.append({'video_id': video_id, 'passed': False})
            else:
                print(f"✓ ({word_count} words)")
                results.append({'video_id': video_id, 'passed': True})
        
        passed = sum(1 for r in results if r['passed'])
        
        print(f"\n{'='*70}")
        print(f"Validation: {passed}/{len(results)} passed")
        print(f"{'='*70}\n")
        
        return results
    
    def run_complete_pipeline(self):
        """Run all steps"""
        
        print(f"\n{'='*80}")
        print("COMPLETE TRANSCRIPTION PIPELINE")
        print(f"Model: {self.model_size} | Device: {self.device}")
        print(f"{'='*80}")
        
        # Step 1: Identify
        videos_needed = self.step1_identify_missing()
        
        if not videos_needed:
            print("\n✅ All videos already have transcripts!")
            return
        
        # Ask for confirmation
        print(f"\nReady to transcribe {len(videos_needed)} videos.")
        print(f"Estimated time: {len(videos_needed)*4/60:.1f} hours (using {self.model_size} model)")
        
        response = input("\nContinue? (y/n): ")
        if response.lower() != 'y':
            print("Cancelled.")
            return
        
        # Step 2: Extract audio
        audio_files = self.step2_extract_audio(videos_needed)
        
        # Step 3: Transcribe
        transcript_results = self.step3_transcribe(audio_files)
        
        # Step 4: Validate
        validation_results = self.step4_validate(videos_needed)
        
        print("\n🎉 Pipeline complete!")


def main():
    parser = argparse.ArgumentParser(
        description='Generate transcripts using Whisper'
    )
    parser.add_argument('--model', default='small',
                       choices=['tiny', 'base', 'small', 'medium', 'large'],
                       help='Whisper model size (default: small)')
    parser.add_argument('--device', default='cuda',
                       choices=['cuda', 'cpu'],
                       help='Processing device (default: cuda)')
    parser.add_argument('--data-dir', default='data/raw_videos',
                       help='Data directory')
    
    args = parser.parse_args()
    
    # Check dependencies
    try:
        import whisper
        import ffmpeg
    except ImportError as e:
        print(f"❌ Missing dependency: {e}")
        print("\nInstall with:")
        print("  pip install openai-whisper")
        print("  # For ffmpeg, see: https://ffmpeg.org/download.html")
        sys.exit(1)
    
    # Run pipeline
    pipeline = CompleteTranscriptionPipeline(
        data_dir=args.data_dir,
        model_size=args.model,
        device=args.device
    )
    
    pipeline.run_complete_pipeline()


if __name__ == '__main__':
    main()
