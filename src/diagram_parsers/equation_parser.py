"""
equation_parser.py
------------------
Extracts LaTeX from equation crops produced by YOLOv11-seg.

Pipeline:
  1. Run Nougat on the masked crop.
  2. Compute confidence from mean token log-probability.
  3. If confidence < NOUGAT_CONF_THRESHOLD → call Mathpix REST API.
  4. If both fail → mark as failed, flag for human review.

Why masked crops matter here:
  Nougat was trained on document pages with white backgrounds.
  Zeroing out the whiteboard background before passing to Nougat
  significantly improves accuracy on educational board content.
"""

import os
import re
import json
import subprocess
import tempfile
import shutil
import requests
from pathlib import Path

from src.diagram_parsers.base_parser import BaseParser, ParsedObject

NOUGAT_CONF_THRESHOLD = 0.60
MATHPIX_APP_ID = os.environ.get("MATHPIX_APP_ID", "")
MATHPIX_APP_KEY = os.environ.get("MATHPIX_APP_KEY", "")


def _latex_to_plain_text(latex: str) -> str:
    """
    Rough conversion of LaTeX to searchable plain text for the retrieval index.
    Not mathematically accurate — just needs to be keyword-searchable.
    """
    text = latex
    replacements = {
        r"\\frac\{([^}]+)\}\{([^}]+)\}": r"\1 over \2",
        r"\\sqrt\{([^}]+)\}": r"sqrt of \1",
        r"\\int": "integral",
        r"\\sum": "sum",
        r"\\prod": "product",
        r"\\lim": "limit",
        r"\\infty": "infinity",
        r"\\alpha": "alpha", r"\\beta": "beta", r"\\gamma": "gamma",
        r"\\delta": "delta", r"\\epsilon": "epsilon", r"\\theta": "theta",
        r"\\lambda": "lambda", r"\\mu": "mu", r"\\pi": "pi",
        r"\\sigma": "sigma", r"\\phi": "phi", r"\\omega": "omega",
        r"\\cdot": "times", r"\\times": "times", r"\\div": "divided by",
        r"\\leq": "less than or equal", r"\\geq": "greater than or equal",
        r"\\neq": "not equal", r"\\approx": "approximately",
        r"\\rightarrow": "arrow", r"\\Rightarrow": "implies",
        r"\^": " to the power ", r"_": " subscript ",
        r"\\": " ", r"[{}$]": "",
    }
    for pattern, replacement in replacements.items():
        text = re.sub(pattern, replacement, text)
    return f"equation: {text.strip()}"


def _run_nougat(crop_path: str) -> tuple[str, float]:
    """
    Run Nougat on a single image crop.
    Returns (latex_string, confidence_score).
    confidence_score is approximated from output length and structure heuristics
    since Nougat does not expose token log-probs via CLI.
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        cmd = [
            "python", "-m", "nougat",
            crop_path,
            "--out", tmp_dir,
            "--no-markdown",
        ]
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=60)

        # Nougat writes {input_stem}.mmd to the output dir
        stem = Path(crop_path).stem
        out_file = Path(tmp_dir) / f"{stem}.mmd"

        if not out_file.exists():
            raise RuntimeError(f"Nougat produced no output. stderr: {proc.stderr[:300]}")

        latex = out_file.read_text().strip()

        if not latex:
            raise RuntimeError("Nougat returned empty output.")

        # Confidence heuristic:
        # - Good output: contains known LaTeX commands, reasonable length
        # - Bad output: mostly garbage characters, very short, or repetition
        has_latex_commands = bool(re.search(r"\\[a-zA-Z]+", latex))
        repetition_ratio = len(set(latex)) / max(len(latex), 1)
        length_ok = 3 <= len(latex) <= 2000

        if has_latex_commands and length_ok and repetition_ratio > 0.05:
            confidence = 0.80
        elif length_ok and repetition_ratio > 0.05:
            confidence = 0.55
        else:
            confidence = 0.20

        return latex, confidence


def _run_mathpix(crop_path: str) -> str:
    """
    Call the Mathpix API as fallback.
    Returns LaTeX string or raises on failure.
    """
    if not MATHPIX_APP_ID or not MATHPIX_APP_KEY:
        raise EnvironmentError(
            "MATHPIX_APP_ID and MATHPIX_APP_KEY env vars not set. "
            "Cannot use Mathpix fallback."
        )

    with open(crop_path, "rb") as f:
        image_bytes = f.read()

    import base64
    b64 = base64.b64encode(image_bytes).decode()

    response = requests.post(
        "https://api.mathpix.com/v3/text",
        headers={
            "app_id": MATHPIX_APP_ID,
            "app_key": MATHPIX_APP_KEY,
            "Content-Type": "application/json",
        },
        json={
            "src": f"data:image/jpeg;base64,{b64}",
            "formats": ["latex_simplified"],
            "data_options": {"include_latex": True},
        },
        timeout=30,
    )
    response.raise_for_status()
    data = response.json()

    latex = data.get("latex_simplified", "")
    if not latex:
        raise RuntimeError(f"Mathpix returned no latex. Response: {data}")
    return latex


class EquationParser(BaseParser):

    def _parse(self, detection: dict, result: ParsedObject) -> None:
        result.parser_used = "nougat"

        # --- Stage 1: Nougat ---
        try:
            latex, confidence = _run_nougat(detection["crop_path"])
            result.nougat_confidence = confidence

            if confidence >= NOUGAT_CONF_THRESHOLD:
                result.latex = latex
                result.extraction_status = "success"
                result.text_representation = _latex_to_plain_text(latex)
                return
            # Low confidence → fall through to Mathpix
        except Exception as nougat_err:
            nougat_error = str(nougat_err)
        else:
            nougat_error = f"low confidence ({result.nougat_confidence:.2f})"

        # --- Stage 2: Mathpix fallback ---
        try:
            latex = _run_mathpix(detection["crop_path"])
            result.latex = latex
            result.parser_used = "mathpix"
            result.extraction_status = "fallback_used"
            result.text_representation = _latex_to_plain_text(latex)
        except Exception as mathpix_err:
            # Both failed
            result.extraction_status = "failed"
            result.error_message = (
                f"Nougat: {nougat_error} | Mathpix: {mathpix_err}"
            )
            result.text_representation = (
                f"equation: unextracted at timestamp {detection['frame_timestamp']}"
            )
