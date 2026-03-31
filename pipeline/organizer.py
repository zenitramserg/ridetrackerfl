"""
organizer.py
Infer the organizing entity (group or shop) from a ride record.
Ported and adapted from weston-rides-monitor legacy project.
"""

import json
import re
from pathlib import Path
from typing import Tuple

CONFIG_PATH = Path(__file__).parent.parent / "config" / "known_locations.json"

def _load_organizers():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)["organizers"]

ORGANIZERS = _load_organizers()


def _normalize(text: str) -> str:
    return re.sub(r"\s+", " ", text.lower().strip())


def infer_organizer(raw_text: str, source_account: str) -> Tuple[str, str, str]:
    """
    Returns (organized_by, organized_by_type, confidence).
    Checks raw visible text first (high confidence), then falls back to account handle.
    """
    text_norm = _normalize(raw_text or "")

    # Search known organizers by alias in raw text
    for org_name, meta in ORGANIZERS.items():
        for alias in meta.get("aliases", []):
            if _normalize(alias) in text_norm:
                return org_name, meta["type"], "high"

    # Fallback: match by account handle
    handle_norm = _normalize(source_account)
    for org_name, meta in ORGANIZERS.items():
        for account in meta.get("accounts", []):
            if _normalize(account) == handle_norm:
                return org_name, meta["type"], "low"

    # Last resort: return account handle as-is
    return source_account, "unknown", "low"


def confidence_rank(confidence: str) -> int:
    return {"high": 2, "low": 1, "": 0}.get((confidence or "").lower(), 0)


def pick_best_organizer(a: dict, b: dict) -> Tuple[str, str, str]:
    """
    Given two ride dicts, return the organizer fields from whichever has higher confidence.
    """
    rank_a = confidence_rank(a.get("organized_by_confidence", ""))
    rank_b = confidence_rank(b.get("organized_by_confidence", ""))

    winner = a if rank_a >= rank_b else b
    return (
        winner.get("organized_by", ""),
        winner.get("organized_by_type", ""),
        winner.get("organized_by_confidence", ""),
    )
