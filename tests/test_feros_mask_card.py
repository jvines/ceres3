"""The CCF mask must be recorded in the product header, not only in filenames.

``sp_type`` decides which mask the cross-correlation uses, and it reaches the
outside world only through the product names (``_XC_G2.pkl``, ``_XCs_G2.pdf``).
Anything reading the FITS header had to guess: ExoAutomata reads
``HIERARCH CERES MASK`` and stored 'unknown' for all 25,694 FEROS epochs it had
ingested. XC_MIN is uninterpretable without it — a G2 mask on an early-type star
produces no dip, which looks identical to a failed reduction.

The pipeline runs at module scope with no importable seam, so the header
assertions parse the source, as tests/test_feros_drift_spline.py does.
"""
from __future__ import annotations

import ast
from pathlib import Path

import pytest

from ceres3.utils import globalutils as G

PIPE = (Path(__file__).resolve().parents[1]
        / "src" / "ceres3" / "instruments" / "ferospipe_fp.py")


@pytest.fixture(scope="module")
def src() -> str:
    return PIPE.read_text()


def test_the_mask_is_written_to_the_header(src):
    assert "'HIERARCH CERES MASK', sp_type" in src


def test_the_card_uses_the_same_value_as_the_product_names(src):
    """A card that disagreed with the .pkl name would be worse than none."""
    tree = ast.parse(src, filename=str(PIPE))
    unparsed = ast.unparse(tree)
    # the pickle and pdf names are built from sp_type ...
    assert "'_XC_' + sp_type" in unparsed
    assert "'_XCs_' + sp_type" in unparsed
    # ... and so is the card
    assert "'HIERARCH CERES MASK', sp_type" in unparsed


def test_the_card_is_written_alongside_the_other_ccf_cards(src):
    assert src.index("'XC_MIN', XC_min") < src.index("'HIERARCH CERES MASK'")
    assert src.index("'HIERARCH CERES MASK'") < src.index("'BJD_OUT', bjd_out")


class TestMaskSelectionContract:
    """What the card can ever contain, and what it defaults to."""

    def test_missing_reffile_falls_back_to_g2(self, tmp_path):
        sp_type, mask = G.get_mask_reffile("HD72673", reffile=str(tmp_path / "nope.txt"))
        assert sp_type == "G2"
        assert mask.endswith("G2.mas")

    def test_target_absent_from_reffile_falls_back_to_g2(self, tmp_path):
        ref = tmp_path / "reffile.txt"
        ref.write_text("SOMEONE_ELSE,0,0,0,0,0,K5\n")
        sp_type, _ = G.get_mask_reffile("HD72673", reffile=str(ref))
        assert sp_type == "G2"

    @pytest.mark.parametrize("declared, expected", [("G2", "G2"), ("K5", "K5"), ("M2", "M2")])
    def test_a_listed_target_gets_its_mask(self, tmp_path, declared, expected):
        ref = tmp_path / "reffile.txt"
        ref.write_text(f"HD72673,0,0,0,0,0,{declared}\n")
        sp_type, mask = G.get_mask_reffile("HD72673", reffile=str(ref))
        assert sp_type == expected
        assert mask.endswith(f"{expected}.mas")

    def test_an_unsupported_mask_name_is_not_honoured(self, tmp_path):
        """Only G2/K5/M2 exist as mask files; anything else must not be returned."""
        ref = tmp_path / "reffile.txt"
        ref.write_text("HD72673,0,0,0,0,0,A0\n")
        sp_type, _ = G.get_mask_reffile("HD72673", reffile=str(ref))
        assert sp_type == "G2"
