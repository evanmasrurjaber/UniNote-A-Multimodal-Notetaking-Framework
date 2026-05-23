"""
graph_parser.py
---------------
Extracts structured chart data from graph crops using MatCha (chart QA)
and DePlot (chart-to-table) from Google via HuggingFace.

Pipeline:
  1. Run MatCha QA to extract chart type, axes, trends.
  2. If chart appears data-heavy (line/bar/scatter) → also run DePlot
     to extract the underlying numeric data table.
  3. Combine into structured output + text_representation for indexing.
"""

import os
import re
from PIL import Image

from src.diagram_parsers.base_parser import BaseParser, ParsedObject

# Lazy-loaded globals — models are large, only loaded once per worker process
_matcha_model = None
_matcha_processor = None
_deplot_model = None
_deplot_processor = None


def _load_matcha():
    global _matcha_model, _matcha_processor
    if _matcha_model is None:
        from transformers import Pix2StructForConditionalGeneration, Pix2StructProcessor
        print("[graph_parser] Loading MatCha model (first call only)...")
        _matcha_model = Pix2StructForConditionalGeneration.from_pretrained(
            "google/matcha-chartqa"
        )
        _matcha_processor = Pix2StructProcessor.from_pretrained(
            "google/matcha-chartqa"
        )
        _matcha_model.eval()
        print("[graph_parser] MatCha loaded.")
    return _matcha_model, _matcha_processor


def _load_deplot():
    global _deplot_model, _deplot_processor
    if _deplot_model is None:
        from transformers import Pix2StructForConditionalGeneration, Pix2StructProcessor
        print("[graph_parser] Loading DePlot model (first call only)...")
        _deplot_model = Pix2StructForConditionalGeneration.from_pretrained(
            "google/deplot"
        )
        _deplot_processor = Pix2StructProcessor.from_pretrained("google/deplot")
        _deplot_model.eval()
        print("[graph_parser] DePlot loaded.")
    return _deplot_model, _deplot_processor


# Structured QA prompts sent to MatCha one at a time
MATCHA_PROMPTS = [
    "What type of chart or graph is this?",
    "What are the x-axis label and units?",
    "What are the y-axis label and units?",
    "What are the data series or variables shown?",
    "What is the main trend, pattern, or key insight from this chart?",
]


def _matcha_qa(image: Image.Image, question: str) -> str:
    """Run a single QA question through MatCha. Returns answer string."""
    import torch
    model, processor = _load_matcha()
    inputs = processor(images=image, text=question, return_tensors="pt")
    with torch.no_grad():
        output = model.generate(**inputs, max_new_tokens=128)
    return processor.decode(output[0], skip_special_tokens=True).strip()


def _deplot_extract(image: Image.Image) -> str:
    """
    Run DePlot to convert chart to a data table (markdown-like string).
    Returns raw DePlot output which is a linearized table.
    """
    import torch
    model, processor = _load_deplot()
    inputs = processor(
        images=image,
        text="Generate underlying data table of the figure below:",
        return_tensors="pt"
    )
    with torch.no_grad():
        output = model.generate(**inputs, max_new_tokens=512)
    return processor.decode(output[0], skip_special_tokens=True).strip()


DATA_HEAVY_TYPES = {"line graph", "bar chart", "scatter plot", "histogram", "line chart"}


def _is_data_heavy(chart_type: str) -> bool:
    return any(t in chart_type.lower() for t in DATA_HEAVY_TYPES)


class GraphParser(BaseParser):

    def _parse(self, detection: dict, result: ParsedObject) -> None:
        result.parser_used = "matcha"
        image = Image.open(detection["crop_path"]).convert("RGB")

        answers = {}
        for prompt in MATCHA_PROMPTS:
            try:
                answers[prompt] = _matcha_qa(image, prompt)
            except Exception as e:
                answers[prompt] = f"[error: {e}]"

        chart_type   = answers[MATCHA_PROMPTS[0]]
        x_axis       = answers[MATCHA_PROMPTS[1]]
        y_axis       = answers[MATCHA_PROMPTS[2]]
        data_series  = answers[MATCHA_PROMPTS[3]]
        key_insight  = answers[MATCHA_PROMPTS[4]]

        result.chart_type  = chart_type
        result.x_axis_label = x_axis
        result.y_axis_label = y_axis
        result.data_series  = [s.strip() for s in data_series.split(",") if s.strip()]
        result.key_insight  = key_insight
        result.structured_description = (
            f"{chart_type} with x-axis '{x_axis}' and y-axis '{y_axis}'. "
            f"Data: {data_series}. Insight: {key_insight}"
        )

        # --- Optional DePlot pass for data-heavy charts ---
        deplot_table = None
        if _is_data_heavy(chart_type):
            try:
                deplot_table = _deplot_extract(image)
                result.parser_used = "matcha+deplot"
            except Exception as deplot_err:
                deplot_table = None  # non-fatal, MatCha output is sufficient

        # --- Build text_representation for retrieval index ---
        parts = [
            f"graph:",
            chart_type,
            f"x-axis {x_axis}",
            f"y-axis {y_axis}",
            data_series,
            key_insight,
        ]
        if deplot_table:
            parts.append(deplot_table[:200])  # truncate for index
        result.text_representation = " ".join(p for p in parts if p)
        result.extraction_status = "success"
