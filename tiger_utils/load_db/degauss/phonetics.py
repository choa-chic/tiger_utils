"""
phonetics.py - Metaphone helpers for degauss TIGER/Line import.

Provides a Python implementation of the metaphone code used in the
original degauss geocoder (metaphon.c). Uses the `metaphone` library's
Double Metaphone implementation and truncates to a requested length
(defaults to 5 characters to match the original SQL `metaphone(name,5)`).
"""
from __future__ import annotations

from typing import Optional

import jellyfish

try:  # Prefer dedicated double metaphone implementation
    from metaphone import doublemetaphone  # type: ignore
except Exception:  # pragma: no cover - fallback when dependency missing
    doublemetaphone = None


def compute_metaphone(value: Optional[str], length: int = 5) -> str:
    """Return a metaphone code capped at ``length`` characters.

    Args:
        value: Input string (street name). ``None`` or empty values return "".
        length: Maximum length of the code (defaults to 5, matching degauss SQL).

    Returns:
        Uppercased metaphone code (possibly empty) trimmed to the requested length.
    """
    if not value:
        return ""

    normalized = value.strip()
    if not normalized:
        return ""

    primary = secondary = None
    if doublemetaphone:
        primary, secondary = doublemetaphone(normalized)
    elif hasattr(jellyfish, "double_metaphone"):
        primary, secondary = jellyfish.double_metaphone(normalized)

    # Prefer primary; fall back to secondary; finally to single metaphone.
    code = primary or secondary or jellyfish.metaphone(normalized)
    if length and length > 0:
        code = (code or "")[:length]
    return (code or "").upper()
