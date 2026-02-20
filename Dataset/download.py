from pathlib import Path
from video_download_pipeline import VideoDownloader

# Resolve paths relative to this script's directory
script_dir = Path(__file__).resolve().parent

# Initialize downloader
downloader = VideoDownloader(output_dir=str(script_dir / 'data' / 'raw_videos'))

# Download test batch
downloader.download_batch(str(script_dir / 'test_urls.csv'))

# Check statistics
downloader.generate_statistics()