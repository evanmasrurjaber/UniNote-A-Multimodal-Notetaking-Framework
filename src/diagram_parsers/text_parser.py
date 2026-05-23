"""
text_parser.py
--------------
Extracts plain text from text-block crops using PaddleOCR.

This handles:
  - Slide titles and bullet points
  - Written definitions on whiteboards
  - Chapter/section headings
  - Any dense text region detected by YOLOv11

PaddleOCR is preferred over Tesseract for this dataset because:
  - Better accuracy on whiteboard handwriting
  - Built-in angle correction (use_angle_cls=True) handles slightly
    tilted text common on physical boards
  - Handles mixed print + handwriting better
"""

from PIL import Image

from src.diagram_parsers.base_parser import BaseParser, ParsedObject

# Confidence threshold below which OCR lines are discarded
OCR_LINE_CONF_THRESHOLD = 0.65

# Lazy-loaded global OCR engine
_ocr_engine = None


def _load_ocr():
    global _ocr_engine
    if _ocr_engine is None:
        from paddleocr import PaddleOCR
        print("[text_parser] Loading PaddleOCR engine...")
        _ocr_engine = PaddleOCR(
            use_angle_cls=True,
            lang="en",
            show_log=False,
        )
        print("[text_parser] PaddleOCR loaded.")
    return _ocr_engine


def _run_ocr(crop_path: str, conf_threshold: float = OCR_LINE_CONF_THRESHOLD) -> list[str]:
    """
    Run PaddleOCR on an image crop.
    Returns list of text lines above confidence threshold, in reading order.
    """
    ocr = _load_ocr()
    result = ocr.ocr(crop_path, cls=True)

    if not result or not result[0]:
        return []

    lines = []
    for line in result[0]:
        # line = [[bbox_points], (text, confidence)]
        text, conf = line[1]
        if conf >= conf_threshold:
            lines.append(text.strip())

    return lines


def _build_text_representation(lines: list[str]) -> str:
    """
    Flatten lines into a searchable string for the retrieval index.
    Strips common LaTeX-like symbols that sneak into OCR output.
    """
    raw = " ".join(lines)
    # Minimal cleanup: collapse whitespace
    cleaned = " ".join(raw.split())
    return f"text block: {cleaned}"


class TextParser(BaseParser):

    def _parse(self, detection: dict, result: ParsedObject) -> None:
        crop_path = detection["crop_path"]
        result.parser_used = "paddleocr"

        lines = _run_ocr(crop_path)

        if not lines:
            # Retry with lower threshold before giving up
            lines = _run_ocr(crop_path, conf_threshold=0.40)

        if lines:
            result.extracted_text = "\n".join(lines)
            result.extraction_status = "success"
            result.text_representation = _build_text_representation(lines)
        else:
            result.extraction_status = "failed"
            result.text_representation = (
                f"text block: unextracted at timestamp {detection['frame_timestamp']}"
            )
