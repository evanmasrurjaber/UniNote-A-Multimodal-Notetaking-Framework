# UniNote Dataset Construction - Summary of Completed Work
## Video Download → Transcripts → Keyframe Extraction

---

## 🎯 PROJECT GOAL

Build a novel multimodal notetaking framework text summerization, visual elements (e.g. diagrams, charts, tables, graphs) and hierarchical chapter segmentation for thesis aims to be published in top conferences or journals.

**Our Thesis**: UniNote - A Multimodal Framework for Automatic Note Generation from Educational Videos

---

## ✅ WHAT YOU'VE COMPLETED (4 MAJOR PHASES)

### Progress Overview
```
✅ Phase 1: Video Download
✅ Phase 2: Transcript Generation
✅ Phase 3: Adaptive Keyframe Extraction
✅ Phase 4: Team Labeling + YOLOv11 Instance Segmentation
```

**Overall Technical Work:**

---

## 📥 PHASE 1: VIDEO DOWNLOAD ✅

### What You Did
Downloaded **150 educational STEM videos** from YouTube with proper metadata and legal compliance.

### Video Sources
- **Khan Academy** (primary source)
- **The Organic Chemistry Tutor** (primary source)
- **3Blue1Brown**
- **MIT OpenCourseWare**
- **CrashCourse**

### Quality Criteria
✅ Resolution: ≥720p
✅ Duration: 5-20 minutes per video
✅ Audio: Clear, minimal background noise
✅ Language: English
✅ Content: Educational lecture/tutorial format
✅ Subtitles: Available (YouTube or generated)

### Technical Challenge Solved
**Problem**: Python 3.14 urllib.parse bug caused yt-dlp to hang on auto-captions

**Solution**: 
- Disabled auto-caption downloads
- Use Whisper for transcription instead (actually better quality!)
- Created Python 3.14-compatible download pipeline

### Tools Created
✅ **video_download_pipeline.py**
   - Downloads videos with metadata
   - Handles Python 3.14 compatibility issues
   - Batch processing with error recovery

### Output Files
```
data/raw_videos/
├── videos/
│   ├── 001_abc123_video.mp4
│   ├── 002_def456_video.mp4
│   └── ... (150 total)
│
├── metadata/
│   └── 001_abc123_metadata.json
│
├── transcripts/
│   ├── 001_abc123_transcript.txt
│   └── 001_abc123_transcript.vtt
│
└── collection_log.json (master metadata)
```


---

## 📝 PHASE 2: TRANSCRIPT GENERATION ✅

### What You Did
Generated complete transcripts for all 150 videos using **OpenAI Whisper** (for videos without YouTube subtitles) and YouTube auto-captions (where available).

### Why Whisper?
After Python 3.14 compatibility issues with YouTube auto-captions, you switched to Whisper:
- ✅ 93-95% accuracy on educational content
- ✅ Free and open-source
- ✅ Handles technical terminology well
- ✅ Automatic punctuation and capitalization
- ✅ Works offline

### Technical Approach

**Model Selection**: Whisper **small** model (best balance)
- Accuracy: 93-95%
- Speed: 6x faster than realtime on GPU
- Size: 460MB
- Training time: ~4 minutes per video

**Processing Pipeline**:
```
Video File → Audio Extraction (FFmpeg) → Whisper Transcription → Text Files
```

### Audio Preprocessing
- Extracted mono audio at 16kHz (Whisper optimal)
- Applied high-pass filter to remove low-frequency noise
- Normalized volume levels
- Saved as WAV format

### Tools Created
1. ✅ **Whisper_Transcript_Generation_Workflow.md**
   - Complete 5-phase transcription workflow
   - Model comparison and selection guide
   - Quality validation procedures

2. ✅ **complete_transcription_pipeline.py**
   - End-to-end automated transcription
   - Audio extraction with FFmpeg
   - Batch processing with progress tracking
   - Automatic quality validation

3. ✅ **QUICKSTART_Whisper_Transcription.md**
   - Simple 2-command setup
   - Quick reference guide
   - Troubleshooting tips

### Output Files
```
data/raw_videos/transcripts/
├── 001_abc123_transcript.txt  (plain text)
├── 001_abc123_transcript.vtt  (with timestamps)
├── 002_def456_transcript.txt
├── 002_def456_transcript.vtt
└── ... (150 videos × 2 formats = 300 files)
```

### Transcript Formats

**TXT Format** (plain text):
```
In this video, we're going to learn about derivatives. 
A derivative measures how a function changes as its 
input changes. Let's start with the definition...
```

**VTT Format** (WebVTT with timestamps):
```
WEBVTT

00:00:05.000 --> 00:00:08.500
In this video, we're going to learn about derivatives.

00:00:08.500 --> 00:00:12.300
A derivative measures how a function changes...
```

---

## 🖼️ PHASE 3: ADAPTIVE KEYFRAME EXTRACTION ✅

### What You Did
Implemented a fully adaptive, per-video keyframe extraction workflow for the full dataset. The pipeline auto-calibrates extraction parameters, captures complete instructional frames, and generates compact label-ready keyframe sets.

### The Challenge
**Problem**: Educational videos evolve content progressively (board writing, diagram buildup, formula derivation), so naive extraction creates:
- Incomplete intermediate frames
- Near-duplicate frames where later frames contain earlier content
- Channel-specific tuning burden that does not generalize across new video styles

### Your Solution: Adaptive, Video-Agnostic Extraction + Labeling Pruning

**Extraction Algorithm (Adaptive)**:
```
1. Warm-up Profiling
   ↓ (sample frames, estimate motion/content distributions)

2. Auto-Calibration
   ↓ (derive motion threshold, stability window, min interval, frame skip)

3. Fused Capture Scoring
   ↓ (motion + content + difference + stability hysteresis)

4. Runtime Guardrails
   ↓ (adjust thresholds to keep keyframe density in target band)

5. Label-Ready Pruning
   ↓ (drop temporal supersets using backward content-union analysis)

6. Metadata Output
   ↓ (store keyframes + keyframes_for_labeling + diagnostics)
```

### Key Work Completed
- Built per-video warm-up calibration for robust parameter estimation
- Applied multi-signal frame quality and difference scoring
- Added runtime density guardrails for stable extraction behavior
- Added temporal redundancy pruning to produce label-ready keyframes
- Configured batch extraction to keep only final label-ready images by default
- Added tuning controls for aggressiveness and pruning sensitivity

### Tools Used
1. ✅ **Dataset/keyframe_extractor.py**
   - Adaptive auto-calibration (no presets)
   - Multi-signal frame quality and difference scoring
   - Runtime density guardrails
   - Label-ready pruning (`keyframes_for_labeling`)
   - Rich extraction metadata and diagnostics

2. ✅ **Dataset/extract_frames.py**
   - Batch adaptive extraction across dataset videos
   - Uses pruning-aware metadata
   - Deletes pruned redundant frame images by default
   - Supports subset processing and tuning arguments

### Output Files
```
data/keyframes/
├── a0406547135d/
│   ├── complete_001_....jpg
│   ├── complete_002_....jpg
│   ├── ... (only label-ready frames kept by batch script)
│   └── extraction_metadata.json
│
├── 79f8b59fe12f/
│   └── ...
│
└── ... (150 video directories)
```

### Metadata Per Video
```json
{
   "video_path": "...",
   "duration": 516.0,
   "fps": 30.0,
   "keyframes_extracted": 30,
   "profile": {
      "mode": "auto",
      "style": "board_like",
      "warmup": {"...": "..."},
      "derived": {"...": "..."}
   },
   "labeling": {
      "enabled": true,
      "input_count": 30,
      "output_count": 9,
      "pruned_count": 21,
      "method": "backward_content_union"
   },
  "keyframes": [
      {"filename": "...", "timestamp": 42.83, "scores": {"...": "..."}},
    ...
   ],
   "keyframes_for_labeling": [
      {"filename": "...", "timestamp": 42.83},
      ...
   ]
}
```

### Achieved Quality Outcomes

**Generalization**:
- Works across board-like, slide-like, and mixed video styles
- No source-specific preset dependency

**Redundancy Reduction**:
- Prunes temporally redundant supersets before labeling
- Produces compact label-ready frame sets

**Data Efficiency**:
- Lower annotation load for the same instructional coverage
- Better downstream labeling throughput and consistency

---

## 🏷️ PHASE 4: ROBOFLOW LABELING + YOLOV11 INSTANCE SEGMENTATION ✅

### What You Did
Completed the full annotation and segmentation training loop with your team:
- Uploaded keyframe images to Roboflow
- Performed collaborative labeling for object detection/segmentation classes
- Trained YOLOv11 Instance Segmentation
- Downloaded labeled dataset exports and trained model weights

### Labeled Classes
- diagram
- equation
- graph
- table
- text

### Team Annotation Workflow
- Created Roboflow project and configured class ontology
- Distributed labeling tasks across team members
- Reviewed and refined annotations collaboratively
- Finalized dataset version for training

### Training Outcome
- Trained **YOLOv11 Instance Segmentation** on the labeled keyframe dataset
- Exported/downloaded:
   - Labeled dataset
   - Model weights

### Downloaded Artifact Paths
- Labeled dataset: `Dataset/data/keyframes_labeled/UniNote.v1i.yolov8/`
- YOLOv11 model weights: `Dataset/data/yolo_model/weights.v1.pt`

### Why This Matters for UniNote
- Provides a trained visual parser for educational board/slide content
- Supplies structured visual objects for fusion with transcripts
- Enables robust multimodal note generation and chapter graph construction
