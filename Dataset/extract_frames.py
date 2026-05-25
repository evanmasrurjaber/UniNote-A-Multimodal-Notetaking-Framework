# batch_extract_keyframes.py
from pathlib import Path
from keyframe_extractor import SmartWhiteboardExtractor, PRESETS
import json

# Load video metadata
with open('data/raw_videos/collection_log.json', 'r') as f:
    metadata = json.load(f)

# Determine preset based on source
for video in metadata['videos']:
    video_id = video['video_id']
    source = video['source']
    video_path = f"data/raw_videos/videos/{video['filename']}"
    output_dir = f"data/keyframes/{video_id}"
    
    # Choose preset
    if 'Khan Academy' in source:
        preset = 'khan_academy'
    elif 'Organic Chemistry Tutor' in source:
        preset = 'organic_chem_tutor'
    else:
        preset = 'default'
    
    print(f"\nProcessing: {video['title']}")
    print(f"Preset: {preset}")
    
    # Extract keyframes
    params = PRESETS[preset]
    extractor = SmartWhiteboardExtractor(**params)
    
    try:
        keyframes = extractor.process_video(video_path, output_dir)
        print(f"✅ Success: {len(keyframes)} frames extracted")
    except Exception as e:
        print(f"❌ Error: {e}")

print("\n Batch processing complete!")