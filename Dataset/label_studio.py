#!/usr/bin/env python3
"""
Prepare annotation tasks for Label Studio
Creates tasks from videos, transcripts, and keyframes
"""

import json
from pathlib import Path

def prepare_label_studio_tasks():
    """
    Create Label Studio import file from your data
    """
    
    print("\n" + "="*70)
    print("PREPARING LABEL STUDIO ANNOTATION TASKS")
    print("="*70 + "\n")
    
    # Paths
    data_dir = Path('data/raw_videos')
    metadata_file = data_dir / 'collection_log.json'
    
    # Load video metadata
    print("Loading video metadata...")
    with open(metadata_file, 'r') as f:
        metadata = json.load(f)
    
    print(f"Found {len(metadata['videos'])} videos\n")
    
    tasks = []
    
    for i, video in enumerate(metadata['videos'], 1):
        video_id = video['video_id']
        video_index = video['video_index']
        
        print(f"[{i}/{len(metadata['videos'])}] Processing: {video['title'][:50]}")
        
        # Video file
        video_path = f"/data/raw_videos/videos/{video['filename']}"
        
        # Transcript
        transcript_txt = data_dir / 'transcripts' / f"{video_index:03d}_{video_id}_transcript.txt"
        transcript = ""
        if transcript_txt.exists():
            with open(transcript_txt, 'r', encoding='utf-8') as f:
                transcript = f.read()
            print(f"  ✓ Transcript loaded ({len(transcript)} chars)")
        else:
            print(f"  ⚠ No transcript found")
        
        # Keyframes
        keyframes_dir = Path(f"data/keyframes/{video_id}")
        keyframes = []
        
        if keyframes_dir.exists():
            # Load keyframe metadata
            kf_metadata_file = keyframes_dir / 'extraction_metadata.json'
            if kf_metadata_file.exists():
                with open(kf_metadata_file, 'r') as f:
                    kf_data = json.load(f)
                
                for kf in kf_data['keyframes']:
                    keyframes.append({
                        'url': f"/data/keyframes/{video_id}/{kf['filename']}",
                        'filename': kf['filename'],
                        'timestamp': kf['timestamp'],
                        'quality': kf.get('quality', 0)
                    })
                
                print(f"  ✓ {len(keyframes)} keyframes loaded")
            else:
                print(f"  ⚠ No keyframe metadata")
        else:
            print(f"  ⚠ No keyframes directory")
        
        # Create task
        task = {
            'data': {
                # Basic info
                'video_id': video_id,
                'video_index': video_index,
                'title': video['title'],
                'subject': video['subject'],
                'difficulty': video['difficulty'],
                'source': video['source'],
                'duration': video['duration'],
                
                # Files
                'video_url': video_path,
                'transcript': transcript,
                'keyframes': keyframes,
                
                # For single keyframe annotation (can switch between)
                'keyframe_url': keyframes[0]['url'] if keyframes else None,
                'keyframe_timestamp': keyframes[0]['timestamp'] if keyframes else 0,
            }
        }
        
        tasks.append(task)
    
    # Save tasks
    output_file = 'label_studio_import.json'
    with open(output_file, 'w') as f:
        json.dump(tasks, f, indent=2)
    
    print(f"\n{'='*70}")
    print(f"✅ CREATED {len(tasks)} ANNOTATION TASKS")
    print(f"{'='*70}")
    print(f"Saved to: {output_file}")
    print(f"File size: {Path(output_file).stat().st_size / 1024:.1f} KB")
    print(f"\nNext steps:")
    print(f"1. Start Label Studio: label-studio start")
    print(f"2. Open http://localhost:8080")
    print(f"3. Create new project")
    print(f"4. Import {output_file}")
    print(f"5. Configure labeling interface with provided XML")
    print(f"{'='*70}\n")
    
    # Generate statistics
    print("Dataset Statistics:")
    print(f"  Total videos: {len(tasks)}")
    
    subjects = {}
    for task in tasks:
        subj = task['data']['subject']
        subjects[subj] = subjects.get(subj, 0) + 1
    
    for subj, count in sorted(subjects.items()):
        print(f"  {subj}: {count}")
    
    print(f"\n  With transcripts: {sum(1 for t in tasks if t['data']['transcript'])}")
    print(f"  With keyframes: {sum(1 for t in tasks if t['data']['keyframes'])}")
    
    avg_keyframes = sum(len(t['data']['keyframes']) for t in tasks) / len(tasks)
    print(f"  Avg keyframes per video: {avg_keyframes:.1f}")
    
    total_duration = sum(t['data']['duration'] for t in tasks)
    print(f"  Total duration: {total_duration/3600:.1f} hours")
    print()
    
    return tasks

if __name__ == '__main__':
    prepare_label_studio_tasks()
