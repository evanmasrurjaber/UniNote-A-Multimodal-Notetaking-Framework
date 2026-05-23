"""
table_parser.py
---------------
Extracts structured tabular data from table crops using PaddleOCR's
PPStructure table recognition engine.

Pipeline:
  1. Run PPStructure on the masked crop to detect table HTML.
  2. Parse HTML → headers + data rows using BeautifulSoup.
  3. Convert to markdown table string.
  4. Fall back to plain OCR text if table structure detection fails.

PPStructure works best on:
  - High-contrast tables with visible border lines
  - Tables on clean white backgrounds (which is why masked crops help)
  - Tables with at least 2 columns and 2 rows
"""

import os
import re
from PIL import Image

from src.diagram_parsers.base_parser import BaseParser, ParsedObject

# Lazy-loaded globals
_table_engine = None


def _load_engine():
    global _table_engine
    if _table_engine is None:
        from paddleocr import PPStructure
        print("[table_parser] Loading PPStructure table engine...")
        _table_engine = PPStructure(
            table=True,
            ocr=True,
            show_log=False,
            lang="en",
        )
        print("[table_parser] PPStructure loaded.")
    return _table_engine


def _html_to_rows(html: str) -> tuple[list, list]:
    """
    Parse HTML table string → (headers, rows).
    Returns ([], []) on parse failure.
    """
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    if not table:
        return [], []

    all_rows = []
    for tr in table.find_all("tr"):
        cells = tr.find_all(["td", "th"])
        all_rows.append([c.get_text(strip=True) for c in cells])

    if not all_rows:
        return [], []

    # Heuristic: first row is headers if it contains <th> elements
    # or if its text differs structurally from subsequent rows
    first_tr = table.find("tr")
    has_th = bool(first_tr and first_tr.find("th"))

    if has_th or len(all_rows) == 1:
        headers = all_rows[0]
        rows = all_rows[1:]
    else:
        headers = all_rows[0]   # treat first row as header anyway
        rows = all_rows[1:]

    return headers, rows


def _rows_to_markdown(headers: list, rows: list) -> str:
    """Convert headers + rows to a GitHub-flavored markdown table string."""
    if not headers:
        return ""

    def pad(text: str, width: int) -> str:
        return text.ljust(width)

    # Compute column widths
    col_widths = [max(len(str(h)), 3) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            if i < len(col_widths):
                col_widths[i] = max(col_widths[i], len(str(cell)))

    header_line = "| " + " | ".join(pad(str(h), col_widths[i])
                                    for i, h in enumerate(headers)) + " |"
    sep_line    = "| " + " | ".join("-" * w for w in col_widths) + " |"
    data_lines  = []
    for row in rows:
        padded = []
        for i, w in enumerate(col_widths):
            cell = str(row[i]) if i < len(row) else ""
            padded.append(pad(cell, w))
        data_lines.append("| " + " | ".join(padded) + " |")

    return "\n".join([header_line, sep_line] + data_lines)


def _fallback_ocr(crop_path: str) -> str:
    """
    Plain PaddleOCR text extraction as fallback when table structure fails.
    Returns raw OCR text.
    """
    from paddleocr import PaddleOCR
    ocr = PaddleOCR(use_angle_cls=True, lang="en", show_log=False)
    result = ocr.ocr(crop_path, cls=True)
    if result and result[0]:
        lines = [line[1][0] for line in result[0] if line[1][1] > 0.65]
        return "\n".join(lines)
    return ""


class TableParser(BaseParser):

    def _parse(self, detection: dict, result: ParsedObject) -> None:
        crop_path = detection["crop_path"]
        engine = _load_engine()

        try:
            structure_result = engine(crop_path)
        except Exception as e:
            raise RuntimeError(f"PPStructure failed: {e}")

        html_table = None
        for region in (structure_result or []):
            if region.get("type") == "table":
                html_table = region.get("res", {}).get("html", "")
                break

        if html_table:
            headers, rows = _html_to_rows(html_table)
            if headers:
                result.headers = headers
                result.rows = rows
                result.markdown_table = _rows_to_markdown(headers, rows)
                result.parser_used = "ppstructure_table"
                result.extraction_status = "success"

                # text_representation: headers + first few rows + concept context
                header_str = " ".join(headers)
                sample_rows = " ".join(
                    " ".join(str(c) for c in r) for r in rows[:3]
                )
                result.text_representation = (
                    f"table: headers {header_str} data {sample_rows}"
                )
                return

        # --- Fallback: plain OCR ---
        raw_text = _fallback_ocr(crop_path)
        if raw_text:
            result.extracted_text = raw_text
            result.parser_used = "paddleocr_plain"
            result.extraction_status = "fallback_used"
            result.text_representation = f"table: {raw_text[:300]}"
        else:
            result.extraction_status = "failed"
            result.text_representation = (
                f"table: unextracted at timestamp {detection['frame_timestamp']}"
            )
