import pytest

from tiger_utils.load_db.degauss.phonetics import compute_metaphone


def test_compute_metaphone_basic():
    # Classic metaphone example; expect primary code returned and uppercased
    assert compute_metaphone("Smith", 5) == "SM0"


def test_compute_metaphone_length_and_empty():
    assert compute_metaphone("Schmidt", 3) == "XMT"
    assert compute_metaphone("", 5) == ""
    assert compute_metaphone(None, 5) == ""
