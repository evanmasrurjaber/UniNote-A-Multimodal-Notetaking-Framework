#!/usr/bin/env python3
"""
UniNote Dataset Builder - Video Download Pipeline (FULLY FIXED)
Works around Python 3.14 urllib.parse bug and JavaScript runtime issues
"""

import yt_dlp
import json
import os
import csv
from pathlib import Path
from datetime import datetime
import hashlib
import time
import sys

class VideoDownloader:
    def __init__(self, output_dir='data/raw_videos'):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Separate directories
        self.video_dir = self.output_dir / 'videos'
        self.metadata_dir = self.output_dir / 'metadata'
        self.transcript_dir = self.output_dir / 'transcripts'
        
        for d in [self.video_dir, self.metadata_dir, self.transcript_dir]:
            d.mkdir(parents=True, exist_ok=True)
        
        self.metadata_file = self.output_dir / 'collection_log.json'
        self.failed_file = self.output_dir / 'failed_downloads.txt'
        
        # Load existing metadata
        if self.metadata_file.exists():
            with open(self.metadata_file, 'r', encoding='utf-8') as f:
                self.collection_data = json.load(f)
        else:
            self.collection_data = {
                'download_date': datetime.now().isoformat(),
                'python_version': sys.version,
                'yt_dlp_version': yt_dlp.version.__version__,
                'total_videos': 0,
                'videos': []
            }
    
    def save_metadata(self):
        """Save metadata to file"""
        with open(self.metadata_file, 'w', encoding='utf-8') as f:
            json.dump(self.collection_data, f, indent=2, ensure_ascii=False)
    
    def download_video(self, url, subject, difficulty, source, video_index):
        """
        Download single video with metadata and transcription
        
        FULLY FIXED for Python 3.14 and JavaScript runtime issues
        """
        
        print(f"\n{'='*80}")
        print(f"Downloading video {video_index}: {url}")
        print(f"Subject: {subject} | Difficulty: {difficulty} | Source: {source}")
        print(f"{'='*80}")
        
        # Generate video ID from URL
        video_id = self._generate_video_id(url)
        
        # Check if already downloaded
        if self._is_already_downloaded(video_id):
            print(f"⚠️  Video {video_id} already downloaded. Skipping...")
            return True
        
        # CRITICAL FIX: Disable automatic captions to avoid Python 3.14 urllib.parse bug
        ydl_opts = {
            # Format selection
            'format': 'bestvideo[height<=720][ext=mp4]+bestaudio[ext=m4a]/best[height<=720][ext=mp4]/best',
            
            # Output template
            'outtmpl': str(self.video_dir / f'{video_index:03d}_{video_id}.%(ext)s'),
            
            # CRITICAL FIX: Only get manual subtitles, NOT auto-generated
            # This avoids the Python 3.14 urllib.parse.urlencode() hang
            'writesubtitles': True,
            'writeautomaticsub': False,  # DISABLED - causes hang in Python 3.14
            'subtitleslangs': ['en'],
            'subtitlesformat': 'vtt',
            
            # Metadata
            'writeinfojson': True,
            
            # Merge format
            'merge_output_format': 'mp4',
            
            # Output control
            'quiet': False,
            'no_warnings': False,
            'ignoreerrors': False,
            
            # Timeouts to prevent hanging
            'socket_timeout': 30,
            
            # User agent
            'http_headers': {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            },
            
            # Postprocessors
            'postprocessors': [{
                'key': 'FFmpegVideoConvertor',
                'preferedformat': 'mp4',
            }],
        }
        
        try:
            # Download video and extract info
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                print("⏳ Extracting video information...")
                info = ydl.extract_info(url, download=True)
                
                # Extract and save metadata
                metadata = self._extract_metadata(info, subject, difficulty, source, video_index)
                
                # Save metadata JSON
                metadata_file = self.metadata_dir / f'{video_index:03d}_{video_id}_metadata.json'
                with open(metadata_file, 'w', encoding='utf-8') as f:
                    json.dump(metadata, f, indent=2, ensure_ascii=False)
                
                # Extract transcript from downloaded files
                has_transcript = self._extract_transcript_from_files(video_index, video_id)
                
                # If no manual subtitles, we'll use Whisper later
                if not has_transcript:
                    print("   ⚠️  No manual subtitles - will need Whisper transcription later")
                    metadata['needs_whisper_transcription'] = True
                else:
                    metadata['needs_whisper_transcription'] = False
                
                # Update metadata with transcript status
                with open(metadata_file, 'w', encoding='utf-8') as f:
                    json.dump(metadata, f, indent=2, ensure_ascii=False)
                
                # Add to collection
                self.collection_data['videos'].append(metadata)
                self.collection_data['total_videos'] += 1
                self.save_metadata()
                
                print(f"✅ Successfully downloaded: {metadata['title']}")
                print(f"   Duration: {metadata['duration']//60}:{metadata['duration']%60:02d}")
                print(f"   Resolution: {metadata['resolution']}")
                print(f"   Has Manual Subtitles: {has_transcript}")
                
                return True
                
        except KeyboardInterrupt:
            print(f"\n⚠️  Download interrupted by user")
            raise
            
        except Exception as e:
            print(f"❌ Failed to download {url}")
            print(f"   Error: {str(e)}")
            import traceback
            traceback.print_exc()
            
            # Log failure
            with open(self.failed_file, 'a', encoding='utf-8') as f:
                f.write(f"{datetime.now().isoformat()}|{url}|{subject}|{str(e)}\n")
            
            return False
    
    def _generate_video_id(self, url):
        """Generate unique ID from URL"""
        return hashlib.md5(url.encode()).hexdigest()[:12]
    
    def _is_already_downloaded(self, video_id):
        """Check if video already downloaded"""
        for video in self.collection_data['videos']:
            if video['video_id'] == video_id:
                return True
        return False
    
    def _extract_metadata(self, info, subject, difficulty, source, video_index):
        """Extract relevant metadata from yt-dlp info"""
        
        metadata = {
            'video_index': video_index,
            'video_id': self._generate_video_id(info.get('webpage_url', info.get('id', ''))),
            'url': info.get('webpage_url', info.get('url', '')),
            'title': info.get('title', 'Unknown'),
            'duration': info.get('duration', 0),
            'upload_date': info.get('upload_date', ''),
            'uploader': info.get('uploader', 'Unknown'),
            'uploader_id': info.get('uploader_id', ''),
            'channel': info.get('channel', 'Unknown'),
            'view_count': info.get('view_count', 0),
            'like_count': info.get('like_count', 0),
            'description': info.get('description', '')[:500] if info.get('description') else '',
            'tags': info.get('tags', [])[:10] if info.get('tags') else [],
            'categories': info.get('categories', []),
            'resolution': f"{info.get('width', 0)}x{info.get('height', 0)}",
            'fps': info.get('fps', 0),
            'vcodec': info.get('vcodec', ''),
            'acodec': info.get('acodec', ''),
            'filesize': info.get('filesize', 0),
            'has_manual_subtitles': 'en' in info.get('subtitles', {}),
            'filename': f"{video_index:03d}_{self._generate_video_id(info.get('webpage_url', ''))}.mp4",
            # Our annotations
            'subject': subject,
            'difficulty': difficulty,
            'source': source,
            'download_date': datetime.now().isoformat(),
        }
        
        return metadata
    
    def _extract_transcript_from_files(self, video_index, video_id):
        """
        Extract transcript from downloaded subtitle files
        """
        
        # Look for downloaded subtitle files
        vtt_files = list(self.video_dir.glob(f'{video_index:03d}_{video_id}*.vtt'))
        
        if not vtt_files:
            return False
        
        try:
            # Use the first VTT file found (manual subtitles)
            vtt_file = vtt_files[0]
            
            # Destination paths
            vtt_dest = self.transcript_dir / f'{video_index:03d}_{video_id}_transcript.vtt'
            txt_dest = self.transcript_dir / f'{video_index:03d}_{video_id}_transcript.txt'
            
            # Read VTT content
            with open(vtt_file, 'r', encoding='utf-8') as f:
                vtt_content = f.read()
            
            # Save VTT copy
            with open(vtt_dest, 'w', encoding='utf-8') as f:
                f.write(vtt_content)
            
            # Parse to plain text
            transcript_text = self._parse_vtt_content(vtt_content)
            
            # Save plain text
            with open(txt_dest, 'w', encoding='utf-8') as f:
                f.write(transcript_text)
            
            print(f"   ✅ Transcript extracted: {len(transcript_text)} characters")
            return True
            
        except Exception as e:
            print(f"   ⚠️  Failed to extract transcript: {str(e)}")
            return False
    
    def _parse_vtt_content(self, vtt_content):
        """Parse VTT content to plain text"""
        
        lines = vtt_content.split('\n')
        transcript = []
        
        for line in lines:
            line = line.strip()
            
            # Skip headers, timestamps, and empty lines
            if (line.startswith('WEBVTT') or 
                '-->' in line or 
                line.isdigit() or 
                not line or
                line.startswith('Kind:') or
                line.startswith('Language:')):
                continue
            
            # Remove VTT tags like <c> </c> <v> </v>
            import re
            line = re.sub(r'<[^>]+>', '', line)
            
            # Skip if empty after tag removal
            if not line.strip():
                continue
            
            transcript.append(line)
        
        return ' '.join(transcript)
    
    def download_batch(self, video_list_file):
        """
        Download videos from CSV file
        CSV format: url,subject,difficulty,source
        """
        
        if not Path(video_list_file).exists():
            print(f"❌ Video list file not found: {video_list_file}")
            return
        
        with open(video_list_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            videos = list(reader)
        
        print(f"\n{'='*80}")
        print(f"BATCH DOWNLOAD: {len(videos)} videos")
        print(f"Python version: {sys.version}")
        print(f"yt-dlp version: {yt_dlp.version.__version__}")
        print(f"\n⚠️  NOTE: Auto-captions disabled due to Python 3.14 compatibility")
        print(f"   Videos without manual subtitles will need Whisper transcription")
        print(f"{'='*80}\n")
        
        success_count = 0
        no_subtitle_count = 0
        start_index = self.collection_data['total_videos'] + 1
        
        for i, row in enumerate(videos, start=start_index):
            try:
                success = self.download_video(
                    url=row['url'],
                    subject=row['subject'],
                    difficulty=row['difficulty'],
                    source=row['source'],
                    video_index=i
                )
                
                if success:
                    success_count += 1
                    # Check if needs Whisper
                    if self.collection_data['videos'][-1].get('needs_whisper_transcription', False):
                        no_subtitle_count += 1
                
                # Rate limiting (be nice to servers)
                if i < len(videos) + start_index - 1:
                    print(f"\n⏳ Waiting 5 seconds before next download...")
                    time.sleep(5)
                    
            except KeyboardInterrupt:
                print(f"\n\n{'='*80}")
                print(f"DOWNLOAD INTERRUPTED BY USER")
                print(f"Progress: {success_count}/{i-start_index+1} videos")
                print(f"{'='*80}\n")
                break
        
        print(f"\n{'='*80}")
        print(f"DOWNLOAD COMPLETE")
        print(f"✅ Success: {success_count}/{len(videos)}")
        print(f"❌ Failed: {len(videos) - success_count}")
        print(f"⚠️  Need Whisper transcription: {no_subtitle_count}")
        print(f"{'='*80}\n")
        
        if no_subtitle_count > 0:
            print(f"📝 Next step: Run Whisper transcription on {no_subtitle_count} videos")
            print(f"   See whisper_transcription.py for instructions\n")
    
    def generate_statistics(self):
        """Generate download statistics"""
        
        if not self.collection_data['videos']:
            print("No videos downloaded yet.")
            return
        
        stats = {
            'total_videos': len(self.collection_data['videos']),
            'by_subject': {},
            'by_difficulty': {},
            'by_source': {},
            'total_duration_hours': 0,
            'avg_duration_minutes': 0,
            'with_manual_subtitles': 0,
            'needs_whisper': 0,
        }
        
        for video in self.collection_data['videos']:
            # Subject distribution
            subject = video['subject']
            stats['by_subject'][subject] = stats['by_subject'].get(subject, 0) + 1
            
            # Difficulty distribution
            difficulty = video['difficulty']
            stats['by_difficulty'][difficulty] = stats['by_difficulty'].get(difficulty, 0) + 1
            
            # Source distribution
            source = video['source']
            stats['by_source'][source] = stats['by_source'].get(source, 0) + 1
            
            # Duration
            stats['total_duration_hours'] += video['duration'] / 3600
            
            # Subtitles
            if video.get('has_manual_subtitles', False):
                stats['with_manual_subtitles'] += 1
            
            if video.get('needs_whisper_transcription', False):
                stats['needs_whisper'] += 1
        
        stats['avg_duration_minutes'] = (stats['total_duration_hours'] * 60) / stats['total_videos']
        
        print(f"\n{'='*80}")
        print("DOWNLOAD STATISTICS")
        print(f"{'='*80}")
        print(f"\nTotal Videos: {stats['total_videos']}")
        print(f"Total Duration: {stats['total_duration_hours']:.1f} hours")
        print(f"Average Duration: {stats['avg_duration_minutes']:.1f} minutes")
        print(f"With Manual Subtitles: {stats['with_manual_subtitles']} ({stats['with_manual_subtitles']/stats['total_videos']*100:.1f}%)")
        print(f"Need Whisper: {stats['needs_whisper']} ({stats['needs_whisper']/stats['total_videos']*100:.1f}%)")
        
        print(f"\nBy Subject:")
        for subject, count in sorted(stats['by_subject'].items()):
            print(f"  {subject}: {count} ({count/stats['total_videos']*100:.1f}%)")
        
        print(f"\nBy Difficulty:")
        for difficulty, count in sorted(stats['by_difficulty'].items()):
            print(f"  {difficulty}: {count} ({count/stats['total_videos']*100:.1f}%)")
        
        print(f"\nBy Source:")
        for source, count in sorted(stats['by_source'].items()):
            print(f"  {source}: {count} ({count/stats['total_videos']*100:.1f}%)")
        
        print(f"\n{'='*80}\n")
        
        # Save statistics
        stats_file = self.output_dir / 'statistics.json'
        with open(stats_file, 'w', encoding='utf-8') as f:
            json.dump(stats, f, indent=2, ensure_ascii=False)
        
        return stats


if __name__ == '__main__':
    # Example usage
    downloader = VideoDownloader(output_dir='data/raw_videos')
    
    print("Video Downloader initialized (Python 3.14 COMPATIBLE)")
    print("Use: downloader.download_batch('video_urls.csv')")
