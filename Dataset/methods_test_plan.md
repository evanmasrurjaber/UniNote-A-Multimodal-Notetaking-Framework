# UniNote: Pilot Testing & Experimental Blueprint (Section 4.2)

## 1. Unified Dataset Strategy (The 3-Video Hybrid Model)
To complete this testing efficiently while maintaining rigorous academic validity, all pilot tests will be conducted on a unified dataset of **3 distinct MIT OCW lectures** (e.g., Mathematics, Physics, and Computer Science).

* **Macro-Evaluation (For Subsystems 4 & 5):** * **Data:** The 3 full-length videos.
    * **Ground Truth:** Manual timestamping of major topic transitions based on MIT OCW PDF page titles. No transcription required.
* **Micro-Evaluation (For Subsystems 1, 2 & 3):**
    * **Data:** Three 5-minute chunks (one from each video, totaling 15 minutes).
    * **Ground Truth:** Manual audio transcription and a tally of all drawn equations/diagrams within those specific 15 minutes.

---

## 2. Experimental Subsystems

### Subsystem 1: Audio Extraction & Transcription
**Objective:** Achieve maximum accuracy on dense STEM vocabulary while retaining precise timestamp alignment for visual synchronization.

* **Methods to Test:**
    1.  **Standard OpenAI Whisper (Base/Small):** The baseline transcription model utilizing standard processing.
        * *Literature Source:* Evaluated as a baseline in Nguyen et al. (2026) and Kuhn et al. (2024). **[Present in Lit Review]**
    2.  **Demucs v4 + Whisper:** Applies an audio separation and noise reduction layer prior to transcription to isolate the lecturer's voice.
        * *Literature Source:* Nguyen et al. (2026) - *Automated speech-to-text captioning for videos and noise robustness analysis*. **[Present in Lit Review]**
    3.  **WhisperX:** Applies forced phonetic alignment using wav2vec2.0 to generate ultra-precise word-level timestamps.
        * *Literature Source:* Bain et al. (2023) - *WhisperX: Time-Accurate Speech Transcription of Long-Form Audio*. **[Needs to be Added]**
* **Why These Specific Methods Were Selected:** We bypassed legacy HMM-GMM models (like Kaldi) because they require extensive acoustic tuning and perform poorly on zero-shot domain-specific accents. Whisper is the current state-of-the-art foundation model for robust ASR. However, Whisper is known to arbitrarily shift timestamps by a few seconds, which fatally desyncs visual alignments. Therefore, we curated this test to evaluate the two leading solutions to Whisper's flaws: pre-processing the audio (Demucs) versus post-processing the alignment (WhisperX).

* **Evaluation Metrics & Execution:**
    1.  **Word Error Rate (WER):** Compare the transcribed text against the manually annotated ground-truth transcript for the 15-minute subset. 
        * *Literature Source:* Standard ASR metric referenced in Kuhn et al. (2024). **[Present in Lit Review]**
    2.  **Timestamp Precision:** Measure the absolute delay (in seconds) between the actual spoken words in the video and the generated VTT timestamps. 
        * *Literature Source:* Evaluation methodology derived from Bain et al. (2023). **[Needs to be Added]**

### Subsystem 2: Keyframe Extraction & Temporal Pruning
**Objective:** Extract significant pedagogical frames (diagrams, math) while mathematically eliminating redundant frames caused by camera pans or human occlusion.

* **Methods to Test:**
    1.  **Pixel-Level/Structural Metrics (SSIM + PySceneDetect):** Traditional extraction relying on detecting differences in raw pixel grids between consecutive frames.
        * *Literature Source:* Utilized in Zhao et al. (2025) for the NoteIt framework. **[Present in Lit Review]**
    2.  **Semantic Thresholding (CLIP embeddings):** Utilizes vision-language embeddings to determine if the semantic meaning of the frame has changed.
        * *Literature Source:* Proposed in Zhao et al. (2025) for semantic-aware extraction. **[Present in Lit Review]**
    3.  **Deep Feature Embeddings (ResNet18 + Cosine Similarity):** Our proposed backward content-union pruning architecture that compares abstract visual features rather than raw pixels.
        * *Literature Source:* He et al. (2016) - *Deep Residual Learning for Image Recognition* (for ResNet architecture basis). **[Needs to be Added]**

* **Why These Specific Methods Were Selected:** We discarded basic frame-differencing as it is too naive, and Optical Flow because it is computationally prohibitive for 1-hour 1080p videos. We selected these three specific methods because they represent the three primary paradigms of computer vision: Classical pixel math (SSIM), Heavyweight multimodal AI (CLIP), and Lightweight convolutional features (ResNet18). Testing them against each other mathematically proves whether the heavy computational cost of modern AI is actually necessary for this specific pruning task.

* **Evaluation Metrics & Execution:**
    1.  **Compression Ratio:** Divide the total number of frames outputted by the algorithm by the raw frame count of the video. 
        * *Literature Source:* Standard video summarization efficiency metric referenced in Kawamura & Rekimoto (2024). **[Present in Lit Review]**
    2.  **Information Yield:** Pass the extracted frames through the downstream YOLO parser. Count if the unique ground-truth equations (from your manual tally) were successfully preserved despite the compression. 
        * *Literature Source:* Proposed custom metric for the UniNote framework.

### Subsystem 3: STEM Visual Element Parsing
**Objective:** Accurately extract text, LaTeX equations, and data points from the pruned keyframes without hallucinating syntax or symbols.

* **Methods to Test:**
    1.  **General Vision-Language Models (GPT-4 Vision / Qwen2-VL):** Feeding the entire raw keyframe into a generalized multimodal model for transcription.
        * *Literature Source:* Evaluated for scientific figures in Gigant et al. (2025) and Chen et al. (2024). **[Present in Lit Review]**
    2.  **Task-Specific Ensemble (YOLOv8 + Nougat/Donut):** Our proposed methodology utilizing a gatekeeper object-detection model to route equations to specialized OCR models.
        * *Literature Source:* Redmon et al. (2016) for YOLO, and Blecher et al. (2023) for Nougat. **[Needs to be Added]**
    3.  **Visual Chain-of-Thought (V-CoT):** Forcing the model to step-by-step interpret charts to improve reasoning integrity.
        * *Literature Source:* Choi et al. (2025) - *End-to-end chart summarization via visual chain-of-thought*. **[Present in Lit Review]**

* **Why These Specific Methods Were Selected:** Traditional OCR (like Tesseract) was excluded entirely because it reads linearly and catastrophically fails on 2D mathematical structures (matrices, fractions). The field is currently split between two competing modern architectures: massive monolithic models (GPT-4V) that try to do everything zero-shot, and specialized routing ensembles (YOLO + Nougat) that act as a mixture-of-experts. We selected these methods to definitively prove which architectural philosophy is safer for academic STEM environments where hallucination is unacceptable.

* **Evaluation Metrics & Execution:**
    1.  **LaTeX Compilation Failure Rate:** Attempt to compile the extracted LaTeX strings into a PDF using standard `pdflatex`. Calculate the percentage that throw syntax errors (e.g., unclosed brackets). 
        * *Literature Source:* Proposed custom structural integrity metric for the UniNote framework.
    2.  **SymPy Equivalence Test:** Subtract the model's output equation from the ground-truth equation using the Python `SymPy` library. If the result is 0, the math is true regardless of formatting differences. 
        * *Literature Source:* Meurer et al. (2017) - *SymPy: symbolic computing in Python* (as the basis for the evaluation tool). **[Needs to be Added]**

### Subsystem 4: Chapter Segmentation & Structuring
**Objective:** Autonomously group the continuous multimodal data into logical, pedagogical hierarchies (Chapters -> Topics -> Sub-topics).

* **Methods to Test:**
    1.  **Text-only Linear Segmentation (TextTiling / Chunking):** Traditional NLP approaches that slice the transcript based purely on keyword shifts and silences.
        * *Literature Source:* Hearst (1997) - *TextTiling: Segmenting Text into Multi-paragraph Subtopic Passages*. **[Needs to be Added]**
    2.  **Multimodal Topic Modeling (BERTopic + Visual Context):** Clusters topics by combining text embeddings with the presence of visual anchors.
        * *Literature Source:* Mukherjee et al. (2022) - *Topic-aware multimodal summarization*. **[Present in Lit Review]**
    3.  **Video-Intrinsic Directed Acyclic Graph (DAG):** Building a hierarchical, non-linear representation of the lecture using LLM routing.
        * *Literature Source:* Zhao et al. (2025) - *NoteIt: A system converting instructional videos to interactable notes*. **[Present in Lit Review]**

* **Why These Specific Methods Were Selected:** We discarded audio-pause segmentation because physical classrooms feature long silences where the professor is writing critical formulas on the board. We selected TextTiling as the classical NLP baseline, BERTopic as the modern embedding baseline, and the DAG structure as the experimental multimodal approach. This tests the hypothesis that educational material is inherently relational and non-linear, and therefore cannot be accurately segmented by traditional linear text chunking.

* **Evaluation Metrics & Execution:**
    1.  **WindowDiff (Page-Title Benchmark):** Compare the algorithm's predicted segment boundaries against the manually recorded timestamps of the MIT OCW PDF page titles. 
        * *Literature Source:* Pevzner & Hearst (2002) - *A Critique and Improvement of an Evaluation Metric for Text Segmentation*. **[Needs to be Added]**
    2.  **Visual Orphan Rate:** Count the percentage of extracted visual diagrams that are erroneously placed in a different segment than their corresponding spoken transcript explanation. 
        * *Literature Source:* Proposed custom multimodal cohesion metric for the UniNote framework.

### Subsystem 5: Multimodal Fusion & Note Generation
**Objective:** Merge the segmented audio transcripts and the parsed visual elements into a readable, final document without factual drift.

* **Methods to Test:**
    1.  **Direct Prompting of an LLM (LLaMA-3 / GPT-4):** Injecting the raw transcripts and visual text directly into a standard context window and asking for a summary.
        * *Literature Source:* Lee et al. (2025) - *Video summarization with large language models*. **[Present in Lit Review]**
    2.  **Multimodal Retrieval-Augmented Generation (mRAG):** Anchoring the generation process by forcing the LLM to map specific retrieved text chunks to localized YOLO visual crops.
        * *Literature Source:* Drushchak et al. (2025) - *Multimodal retrieval-augmented generation: Unified information processing*. **[Present in Lit Review]**

* **Why These Specific Methods Were Selected:** Traditional extractive summarization algorithms (like TF-IDF or TextRank) were excluded because they cannot fuse or reference multimodal visual assets. Generative LLMs are strictly required for this task. We are testing these two specific methods because they represent the only two ways to use LLMs for this pipeline: either hoping the LLM figures out the alignment internally (Direct Prompting), or algorithmically forcing the alignment via external memory (mRAG).


* **Evaluation Metrics & Execution:**
    1.  **BERTScore:** Calculate the deep semantic cosine similarity between the generated note paragraphs and the MIT OCW PDF ground truth to evaluate coverage beyond exact word matching.
        * *Literature Source:* Zhang et al. (2019) - *BERTScore: Evaluating Text Generation with BERT*. **[Needs to be Added]**
    2.  **Cross-Modal Grounding Check ($\Delta t$ Penalty):** Measure the absolute time difference ($\Delta t$) between the video origin timestamp of the generated text paragraph and the origin timestamp of the injected visual asset. 
        * *Literature Source:* Metric adapted from temporal grounding evaluations in Shin et al. (2025) and Vidi Team (2026). **[Present in Lit Review]**