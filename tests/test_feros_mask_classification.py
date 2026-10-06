"""A star without a given mask is correlated with the mask its Coelho Teff calls for.

ExoAutomata classifies every FEROS frame (-do_class), but the mask ignored the
result: the targets file's mask, else G2 (and original CERES read a hand-written
reffile). Now a target the targets file gives no mask takes it from the frame's
classification Teff, with ExoAutomata's boundaries; G2 remains only for a frame
whose classification did not run or failed (EXOAUTOMAT-308).
"""
from __future__ import annotations

import ast
from pathlib import Path

import pytest

from ceres3.instruments import ferosutils_fp as U

PIPE = (Path(__file__).resolve().parents[1]
        / "src" / "ceres3" / "instruments" / "ferospipe_fp.py")


@pytest.mark.parametrize("teff, mask", [
    (6500, "G2"), (5778, "G2"), (5200, "G2"), (5199.9, "K5"), (4500, "K5"), (3900, "K5"),
    (3899, "M2"), (3200, "M2"), (-999, None), (0, None), (float("nan"), None), (None, None), ("x", None),
])
def test_mask_for_teff(teff, mask):
    assert U.mask_for_teff(teff) == mask


def test_the_boundaries_match_exoautomata():
    # backend/app/services/ceres/targets.py: G2_MIN_TEFF = 5200, K5_MIN_TEFF = 3900
    assert (U.MASK_G2_MIN_TEFF, U.MASK_K5_MIN_TEFF) == (5200.0, 3900.0)


def test_the_pipeline_falls_back_to_the_classification_then_g2():
    src = ast.unparse(ast.parse(PIPE.read_text(), filename=str(PIPE)))
    block = src[src.index("if _target is not None and _target.mask:"):]
    block = block[:block.index("mask = _xc_masks_dir + sp_type + '.mas'")]
    t = block.index("sp_type, mask_source = (_target.mask, 'targets')")
    c = block.index("sp_type, mask_source = (ferosutils_fp.mask_for_teff(T_eff), 'classification')")
    d = block.index("sp_type, mask_source = ('G2', 'default')")
    assert t < c < d
    # the classification Teff exists by then
    assert src.index("T_eff_epoch = T_eff") < src.index("if _target is not None and _target.mask:")
