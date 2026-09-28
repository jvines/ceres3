"""An RV that lies outside the velocities the CCF was computed over is not a
measurement of the star, and the product must say so (EXOAUTOMAT-272).

``RV`` is the centre of an unconstrained least-squares fit to the averaged CCF,
while the CCF is only sampled over ``vels``: the coarse search is bounded by
``velw = 300`` km/s and the fine one by ``vel0_xc +- max(20, 6*disp)``, so the
grid never reaches much past ~570 km/s. On faint, fast-rotating frames the fit
walked far outside it and still reported a photon-noise error with its 2 m/s
floor. 2018-01-25 HATS602-066: ``RV = 6008.8 +- 0.0020`` km/s at SNR 6, on a
frame whose wavelength solution was healthy (drift measured to 2.0 m/s) and
whose CCF dip was deep (XC_MIN 0.46) -- so no amount of wavelength-solution
grading can catch it. 18 such RVs sat in the archive marked good.

The pipeline runs at module scope with no importable seam, so these assert
against the parsed source, as tests/test_feros_drift_spline.py does.
"""
from __future__ import annotations

import ast
from pathlib import Path

import numpy as np
import pytest

PIPE = (Path(__file__).resolve().parents[1]
        / "src" / "ceres3" / "instruments" / "ferospipe_fp.py")


@pytest.fixture(scope="module")
def src() -> str:
    return PIPE.read_text()


@pytest.fixture(scope="module")
def tree(src) -> ast.Module:
    return ast.parse(src, filename=str(PIPE))


def test_the_fit_centre_is_checked_against_the_sampled_velocities(src):
    assert "rv_in_grid" in src
    # the check must compare the reported centre with the CCF's own grid
    assert "vels.min() <= p1gau_m[1] <= vels.max()" in src


def test_the_check_runs_before_rv_is_rounded_into_the_product(src):
    assert src.index("rv_in_grid =") < src.index("RV     = np.around(p1gau_m[1],4)")


def test_both_cards_are_written(src):
    assert "'HIERARCH GOOD QUALITY RV', rv_in_grid" in src
    assert "'HIERARCH RV FLAG REASON', rv_flag_reason" in src


def test_an_out_of_grid_fit_warns_on_the_job(src):
    i = src.index("if not rv_in_grid:")
    block = src[i:i + 1200]
    assert "_pipeline_warnings.append" in block
    assert "not a measurement of this star" in block


@pytest.mark.parametrize("centre, lo, hi, ok", [
    (-16.6, -300.0, 300.0, True),      # tau Ceti, comfortably inside
    (6008.8, -270.0, 270.0, False),    # HATS602-066, the real case
    (-6710.9, -300.0, 300.0, False),
    (300.0, -300.0, 300.0, True),      # the edge counts as sampled
    (float("nan"), -300.0, 300.0, False),
])
def test_the_rule_itself(centre, lo, hi, ok):
    vels = np.linspace(lo, hi, 601)
    assert bool(np.isfinite(centre) and vels.min() <= centre <= vels.max()) is ok


def test_the_reason_fits_a_fits_card(src):
    # HIERARCH cards have limited room; the reason is truncated to 60 chars
    i = src.index("rv_flag_reason = (f'CCF fit centre")
    assert "[:60]" in src[i:i + 400]
