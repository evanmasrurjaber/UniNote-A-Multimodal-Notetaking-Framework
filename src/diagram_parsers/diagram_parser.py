"""
diagram_parser.py
-----------------
Describes diagram crops using InternVL2-8B in zero-shot mode.
Falls back to GPT-4o Vision API if InternVL2 is unavailable.

Output structure:
  - description: 2-3 sentence summary of what is shown
  - key_entities: list of labeled components / objects in the diagram
  - relationships: how those entities interact or connect

The model is loaded once per worker process (lazy singleton).
"""

import os
import json
import re
import base64
from PIL import Image

from src.diagram_parsers.base_parser import BaseParser, ParsedObject

# Environment
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
HF_TOKEN = os.environ.get("HF_TOKEN", "")

# Lazy-loaded InternVL2 globals
_internvl_model = None
_internvl_tokenizer = None


def _load_internvl():
    """Load InternVL2-8B. Requires HF_TOKEN and ~16GB VRAM."""
    global _internvl_model, _internvl_tokenizer
    if _internvl_model is None:
        import torch
        from transformers import AutoTokenizer, AutoModel
        print("[diagram_parser] Loading InternVL2-8B (first call only)...")
        _internvl_tokenizer = AutoTokenizer.from_pretrained(
            "OpenGVLab/InternVL2-8B",
            trust_remote_code=True,
            token=HF_TOKEN,
        )
        _internvl_model = AutoModel.from_pretrained(
            "OpenGVLab/InternVL2-8B",
            torch_dtype=torch.bfloat16,
            device_map="auto",
            trust_remote_code=True,
            token=HF_TOKEN,
        ).eval()
        print("[diagram_parser] InternVL2-8B loaded.")
    return _internvl_model, _internvl_tokenizer


SYSTEM_PROMPT = (
    "You are a STEM education expert analyzing a cropped region from an educational "
    "video keyframe. The image shows a single diagram, figure, or illustration. "
    "Respond ONLY in valid JSON with exactly these keys: "
    '"description", "key_entities", "relationships". '
    "No text outside the JSON object. "
    '"description" is a string (2-3 sentences). '
    '"key_entities" is a list of strings. '
    '"relationships" is a list of strings.'
)

USER_PROMPT = (
    "Analyze this educational diagram. Provide:\n"
    "1. description: What is shown? (2-3 specific sentences, name it precisely)\n"
    "2. key_entities: All labeled components, forces, molecules, or objects visible\n"
    "3. relationships: How entities interact, connect, or relate to each other"
)


def _parse_json_response(raw: str) -> dict:
    """
    Extract JSON from model output, handling markdown code fences
    and other common wrapping patterns.
    """
    # Strip markdown fences
    cleaned = re.sub(r"```(?:json)?", "", raw).strip().rstrip("`").strip()
    # Find first { ... } block
    match = re.search(r"\{.*\}", cleaned, re.DOTALL)
    if match:
        return json.loads(match.group())
    raise ValueError(f"No JSON object found in response: {raw[:200]}")


def _run_internvl2(image: Image.Image) -> dict:
    """Run InternVL2-8B on an image and return parsed JSON response."""
    import torch
    from transformers import CLIPImageProcessor

    model, tokenizer = _load_internvl()

    # InternVL2 uses its own pixel_values preprocessing
    from torchvision import transforms
    transform = transforms.Compose([
        transforms.Resize((448, 448)),
        transforms.ToTensor(),
        transforms.Normalize(mean=[0.485, 0.456, 0.406],
                             std=[0.229, 0.224, 0.225]),
    ])
    pixel_values = transform(image.convert("RGB")).unsqueeze(0)
    pixel_values = pixel_values.to(torch.bfloat16).to(model.device)

    full_prompt = f"{SYSTEM_PROMPT}\n\n{USER_PROMPT}"

    generation_config = {"max_new_tokens": 512, "do_sample": False}
    response = model.chat(
        tokenizer,
        pixel_values,
        full_prompt,
        generation_config,
    )
    return _parse_json_response(response)


def _run_gpt4o_vision(crop_path: str) -> dict:
    """GPT-4o vision fallback via OpenAI API."""
    if not OPENAI_API_KEY:
        raise EnvironmentError("OPENAI_API_KEY not set. Cannot use GPT-4o fallback.")

    import requests as req
    with open(crop_path, "rb") as f:
        b64 = base64.b64encode(f.read()).decode()

    response = req.post(
        "https://api.openai.com/v1/chat/completions",
        headers={"Authorization": f"Bearer {OPENAI_API_KEY}",
                 "Content-Type": "application/json"},
        json={
            "model": "gpt-4o",
            "max_tokens": 512,
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": USER_PROMPT},
                        {"type": "image_url",
                         "image_url": {"url": f"data:image/jpeg;base64,{b64}"}},
                    ],
                },
            ],
        },
        timeout=30,
    )
    response.raise_for_status()
    raw = response.json()["choices"][0]["message"]["content"]
    return _parse_json_response(raw)


class DiagramParser(BaseParser):

    def _parse(self, detection: dict, result: ParsedObject) -> None:
        image = Image.open(detection["crop_path"]).convert("RGB")

        # Try InternVL2 first, fall back to GPT-4o
        parsed = None
        try:
            parsed = _run_internvl2(image)
            result.parser_used = "internvl2"
        except Exception as internvl_err:
            try:
                parsed = _run_gpt4o_vision(detection["crop_path"])
                result.parser_used = "gpt4o_vision"
                result.extraction_status = "fallback_used"
            except Exception as gpt_err:
                raise RuntimeError(
                    f"InternVL2: {internvl_err} | GPT-4o: {gpt_err}"
                )

        result.description   = parsed.get("description", "")
        result.key_entities  = parsed.get("key_entities", [])
        result.relationships = parsed.get("relationships", [])

        if result.extraction_status != "fallback_used":
            result.extraction_status = "success"

        # Build text_representation for retrieval index
        entities_str = " ".join(result.key_entities)
        relations_str = " ".join(result.relationships)
        result.text_representation = (
            f"diagram: {result.description} entities: {entities_str} "
            f"relationships: {relations_str}"
        )
