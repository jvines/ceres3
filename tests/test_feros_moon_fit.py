"""The two-Gaussian (scattered moonlight) CCF fit is used only where the moon can matter.

ceres3 had hard-coded the moon model on for every frame. Over 35,597 archive CCFs
that moved the RV by more than 10 m/s on 40% of frames and by more than 1.5 km/s
on 10%, nearly all of them frames whose CCF the moon could not reach. The model is
now applied when the moon velocity lies within MOON_NSIGMA of the single-Gaussian
CCF fit, or when moon_corr.txt asks for it, as in the original CERES.

The pipeline runs at module scope with no importable seam, so the wiring is checked
on the source, as tests/test_feros_mask_card.py does; the rule itself is a helper.
"""
from __future__ import annotations

import ast
import math
from pathlib import Path

import pytest

from ceres3.instruments import ferosutils_fp as U

PIPE = (Path(__file__).resolve().parents[1]
        / "src" / "ceres3" / "instruments" / "ferospipe_fp.py")


@pytest.fixture(scope="module")
def src() -> str:
    return ast.unparse(ast.parse(PIPE.read_text(), filename=str(PIPE)))


class TestRule:
    def test_threshold_is_three_sigma(self):
        assert U.MOON_NSIGMA == 3.0

    @pytest.mark.parametrize("moon, rv, sigma, expected", [
        (20.0, 14.8, 4.0, True),     # 1.3 sigma away: overlaps the stellar CCF
        (26.7, 14.8, 4.0, True),     # 2.975 sigma
        (26.8, 14.8, 4.0, False),    # exactly 3 sigma: not within
        (60.0, 14.8, 4.0, False),    # far away: the single Gaussian stands
        (-5.0, 14.8, 4.0, False),    # 4.95 sigma on the other side
        (14.8, 14.8, 4.0, True),     # moon on top of the star
    ])
    def test_moon_within_three_sigma_of_the_ccf(self, moon, rv, sigma, expected):
        assert U.moon_contaminates(moon, rv, sigma) is expected

    def test_separation_is_in_units_of_the_ccf_sigma(self):
        assert U.moon_separation(22.8, 14.8, 4.0) == pytest.approx(2.0)
        assert U.moon_separation(6.8, 14.8, 4.0) == pytest.approx(2.0)

    @pytest.mark.parametrize("moon, rv, sigma", [
        (20.0, 14.8, 0.0), (20.0, 14.8, -1.0), (20.0, 14.8, float("nan")),
        (20.0, float("nan"), 4.0), (float("nan"), 14.8, 4.0), (None, 14.8, 4.0),
    ])
    def test_a_failed_single_gaussian_fit_never_triggers_the_moon_model(self, moon, rv, sigma):
        assert math.isinf(U.moon_separation(moon, rv, sigma))
        assert U.moon_contaminates(moon, rv, sigma) is False

    def test_the_threshold_can_be_overridden(self):
        assert U.moon_contaminates(30.0, 14.8, 4.0, nsigma=4.0) is True
        assert U.moon_contaminates(30.0, 14.8, 4.0) is False


class TestWiring:
    def test_the_moon_model_is_no_longer_forced(self, src):
        assert "here_moon = True" not in src
        assert "know_moon = True\n        here_moon = True" not in src

    def test_moon_corr_still_forces_it_per_frame(self, src):
        assert "if fsim.split('/')[-1] in spec_moon:" in src
        assert "here_moon = bool(use_moon[I][0])" in src

    def test_the_rule_gates_the_two_gaussian_fit(self, src):
        assert ("if know_moon and here_moon or "
                "ferosutils_fp.moon_contaminates(refvel, p1gau[1], p1gau[2]):") in src

    def test_the_rule_uses_the_single_gaussian_fit(self, src):
        # the separation must come from the moon=False fit, before p1gau_m exists
        single = src.index("moon=False)")
        rule = src.index("ferosutils_fp.moon_contaminates(refvel, p1gau[1], p1gau[2])")
        double = src.index("moon=True)")
        assert single < rule < double

    def test_the_decision_is_recorded_in_the_header(self, src):
        assert "'HIERARCH CERES MOON FIT', bool(moon_flag)" in src
        assert "'HIERARCH CERES MOON SEP', moon_sep" in src
