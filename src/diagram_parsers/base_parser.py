"""
base_parser.py
--------------
Abstract base class that every specialist parser inherits from.
Enforces a consistent output schema across all 5 object classes.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field, asdict
from typing import Optional
import time
import traceback


@dataclass
class ParsedObject:
    """
    Canonical output schema for every parsed visual object.
    Every parser must populate all required fields.
    Optional fields are class-specific.
    """
    # --- Identity (copied from detection) ---
    detection_id: str                    # e.g. "abc123_t45.23_equation_0"
    class_name: str                      # "equation" | "graph" | "diagram" | "table" | "text"
    crop_path: str                       # path to the masked crop image
    video_id: str
    frame_timestamp: float

    # --- Extraction status ---
    extraction_status: str = "success"   # "success" | "failed" | "fallback_used" | "skipped"
    parser_used: str = ""                # e.g. "nougat", "mathpix", "matcha", "internvl2"
    parse_time_sec: float = 0.0

    # --- Retrieval representation (required for FAISS + BM25 index) ---
    text_representation: str = ""        # plain-text version of content, used for indexing

    # --- Class-specific fields (None if not applicable) ---
    latex: Optional[str] = None                          # equation only
    nougat_confidence: Optional[float] = None            # equation only

    chart_type: Optional[str] = None                     # graph only
    x_axis_label: Optional[str] = None                   # graph only
    y_axis_label: Optional[str] = None                   # graph only
    data_series: Optional[list] = field(default=None)    # graph only
    key_insight: Optional[str] = None                    # graph only
    structured_description: Optional[str] = None         # graph only

    description: Optional[str] = None                   # diagram only
    key_entities: Optional[list] = field(default=None)  # diagram only
    relationships: Optional[list] = field(default=None) # diagram only

    headers: Optional[list] = field(default=None)        # table only
    rows: Optional[list] = field(default=None)           # table only
    markdown_table: Optional[str] = None                 # table only

    extracted_text: Optional[str] = None                 # text block only

    error_message: Optional[str] = None                  # populated on failure

    def to_dict(self) -> dict:
        return asdict(self)


class BaseParser(ABC):
    """
    All specialist parsers inherit this. Subclasses implement _parse().
    The public parse() method wraps _parse() with timing and error handling.
    """

    def parse(self, detection: dict) -> ParsedObject:
        start = time.time()
        result = ParsedObject(
            detection_id=detection["detection_id"],
            class_name=detection["class_name"],
            crop_path=detection["crop_path"],
            video_id=detection["video_id"],
            frame_timestamp=detection["frame_timestamp"],
        )
        try:
            self._parse(detection, result)
        except Exception as e:
            result.extraction_status = "failed"
            result.error_message = traceback.format_exc()
            result.text_representation = f"{detection['class_name']}: extraction failed"
        finally:
            result.parse_time_sec = round(time.time() - start, 3)
        return result

    @abstractmethod
    def _parse(self, detection: dict, result: ParsedObject) -> None:
        """
        Populate `result` in-place. Raise on unrecoverable failure.
        Never return a value — mutate result directly.
        """
        ...
