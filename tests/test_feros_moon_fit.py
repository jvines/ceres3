"""When the two-Gaussian (scattered moonlight) CCF fit is used, and when the RV is flagged.

EXOAUTOMAT-301. The fit, with the moon's mean fixed at the moon velocity, its depth
>= 0 and its width free, recovers the stellar RV only when the moon dip is deep
enough to measure and resolved from the star's. On real archive CCFs with injected
moonlight it otherwise fits the star's own departure from a Gaussian (a shallow
moon: worse than no fit) or trades moon depth for stellar centre (an unresolved
moon: half the bias left). So the moonlight is predicted, the fit is tried only
where that moonlight can bias the RV, it is kept only when it measured the moon,
and an RV still biased beyond its error is flagged.

The pipeline runs at module scope with no importable seam, so the wiring is checked
on the source, as tests/test_feros_mask_card.py does; the rule itself is in helpers.
"""
from __future__ import annotations

import ast
import math
from pathlib import Path

import numpy as np
import pytest

from ceres3.instruments import ferosutils_fp as U
from ceres3.utils import globalutils as G

PIPE = (Path(__file__).resolve().parents[1]
        / "src" / "ceres3" / "instruments" / "ferospipe_fp.py")
TAU_CETI_2020_12_13 = 59197.02182915393      # mid-exposure; the Moon had set


@pytest.fixture(scope="module")
def src() -> str:
    return ast.unparse(ast.parse(PIPE.read_text(), filename=str(PIPE)))


def ccf(vels, depth=0.33, mu=0.0, sigma=4.0, moon_depth=0.0, moon_vel=0.0, moon_sigma=4.0):
    return (1.0 - depth * np.exp(-0.5 * ((vels - mu) / sigma) ** 2)
            - moon_depth * np.exp(-0.5 * ((vels - moon_vel) / moon_sigma) ** 2))


def p1gau_of(depth=0.33, mu=0.0, sigma=4.0):
    """The XC_Final_Fit parametrisation of a Gaussian dip: amplitude, centre, width."""
    return np.array([-depth * sigma * np.sqrt(2 * np.pi), mu, sigma])


class TestSeparation:
    def test_is_in_units_of_the_ccf_sigma(self):
        assert U.moon_separation(22.8, 14.8, 4.0) == pytest.approx(2.0)
        assert U.moon_separation(6.8, 14.8, 4.0) == pytest.approx(2.0)

    @pytest.mark.parametrize("moon, rv, sigma", [
        (20.0, 14.8, 0.0), (20.0, 14.8, -1.0), (20.0, 14.8, float("nan")),
        (20.0, float("nan"), 4.0), (float("nan"), 14.8, 4.0), (None, 14.8, 4.0),
    ])
    def test_a_failed_single_gaussian_fit_gives_no_separation(self, moon, rv, sigma):
        assert math.isinf(U.moon_separation(moon, rv, sigma))


class TestGateAndFlag:
    def test_the_fit_is_tried_when_the_bias_exceeds_a_third_of_the_error(self):
        assert U.moon_can_bias(0.0034, 0.010) is True
        assert U.moon_can_bias(-0.0034, 0.010) is True
        assert U.moon_can_bias(0.0033, 0.010) is False

    def test_the_rv_is_flagged_when_the_bias_exceeds_its_error(self):
        assert U.moon_flags_rv(0.0101, 0.010) is True
        assert U.moon_flags_rv(-0.0101, 0.010) is True
        assert U.moon_flags_rv(0.0099, 0.010) is False

    @pytest.mark.parametrize("fn", [U.moon_can_bias, U.moon_flags_rv])
    def test_no_usable_error_never_triggers(self, fn):
        assert fn(1.0, 0.0) is False
        assert fn(1.0, -999.0) is False


class TestAcceptance:
    P1 = p1gau_of(mu=0.0, sigma=4.0)

    @staticmethod
    def fitted(moon_depth, moon_sigma, star_amp=-3.3):
        return np.array([star_amp, 0.0, 4.0, moon_depth / -star_amp, moon_sigma])

    def test_a_deep_resolved_moon_at_the_predicted_depth_is_kept(self):
        assert U.accept_moon_fit(self.P1, self.fitted(0.03, 4.3), 8.0, 0.03) == (True, '')

    def test_an_unresolved_moon_is_not(self):
        ok, why = U.accept_moon_fit(self.P1, self.fitted(0.03, 4.3), 6.0, 0.03)   # 1.5 sigma
        assert not ok and 'unresolved' in why

    @pytest.mark.parametrize("depth, predicted", [(0.011, 0.001), (0.0032, 0.01)])
    def test_a_moon_far_from_the_predicted_depth_is_not(self, depth, predicted):
        ok, why = U.accept_moon_fit(self.P1, self.fitted(depth, 4.0), 8.0, predicted)
        assert not ok and 'prediction' in why

    @pytest.mark.parametrize("width", [1.5, 10.7])
    def test_a_moon_without_the_width_of_sunlight_is_not(self, width):
        ok, why = U.accept_moon_fit(self.P1, self.fitted(0.03, width), 8.0, 0.03)
        assert not ok and 'width' in why

    def test_no_predicted_moonlight_is_never_kept(self):
        ok, _ = U.accept_moon_fit(self.P1, self.fitted(0.03, 4.0), 8.0, 0.0)
        assert not ok

    def test_the_moon_component_is_read_from_the_bounded_fit(self):
        assert U.moon_component(self.fitted(0.03, 4.3)) == pytest.approx((0.03, 4.3))
        assert U.moon_component(np.array([-3.3, 0.0, 4.0])) == (0.0, 0.0)


class TestBoundedFit:
    V = np.arange(-30.0, 30.0, 0.2)

    def test_a_deep_resolved_moon_is_recovered(self):
        y = ccf(self.V, moon_depth=0.03, moon_vel=9.0)
        _, _, p, _, _ = G.XC_Final_Fit(self.V, y, sigma_res=4, horder=8, moonv=9.0, moons=4.0,
                                       moon=True, moon_bounded=True, moon_depth0=0.02)
        assert p[1] == pytest.approx(0.0, abs=0.005)
        assert U.moon_component(p) == pytest.approx((0.03, 4.0), rel=0.05)

    def test_the_moon_is_never_fitted_as_emission(self):
        # A bump where the moon should be: the unbounded CERES fit puts an emission
        # component there (what happened on tau Ceti 2020-12-13); the bounded one cannot.
        y = ccf(self.V, moon_depth=-0.01, moon_vel=6.0)
        _, _, p, _, _ = G.XC_Final_Fit(self.V, y, sigma_res=4, horder=8, moonv=6.0, moons=4.0,
                                       moon=True, moon_bounded=True, moon_depth0=0.01)
        assert U.moon_component(p)[0] >= 0.0
        _, _, legacy, _, _ = G.XC_Final_Fit(self.V, y, sigma_res=4, horder=8, moonv=6.0, moons=4.0, moon=True)
        assert -legacy[0] * legacy[3] < 0.0

    def test_the_legacy_fit_is_unchanged_for_other_instruments(self):
        y = ccf(self.V, moon_depth=0.03, moon_vel=9.0)
        _, _, p, _, _ = G.XC_Final_Fit(self.V, y, sigma_res=4, horder=8, moonv=9.0, moons=4.0, moon=True)
        assert len(p) == 4


class TestPrediction:
    V = np.arange(-30.0, 30.0, 0.2)

    def test_no_moonlight_once_the_moon_has_set(self):
        pred = U.predict_moon(TAU_CETI_2020_12_13, 26.0066, -15.9325, 2000.0, self.V, p1gau_of(), -24.87)
        assert pred['moon_alt'] < 0
        assert math.isinf(pred['sky_mag'])
        assert pred['ratio'] == 0.0 and pred['depth'] == 0.0 and pred['bias'] == 0.0

    def test_a_failed_prediction_never_raises(self):
        pred = U.predict_moon(float('nan'), 26.0, -15.9, 2000.0, self.V, p1gau_of(), 0.0)
        assert pred['bias'] == 0.0

    def test_the_rv_error_is_ceres_s(self):
        p = p1gau_of(depth=0.33, sigma=4.0)
        A, B = 0.11081, 0.0016
        depth_fact = (1 - 0.6) / (1 - max(0.6, 1 + p[0] / (p[2] * np.sqrt(2 * np.pi))))
        assert U.ccf_rv_error('G2', p, 100.0) == pytest.approx((B + (1.6 + 0.8) * A / 100) * depth_fact)
        assert U.ccf_rv_error('K5', p, 1e9) == pytest.approx(0.00311 * depth_fact)
        assert U.ccf_rv_error('G2', np.array([0.1, 0.0, 4.0]), 100.0) == 0.002   # no dip: CERES's floor

    def test_the_count_rate_is_read_near_5500(self):
        spec = np.zeros((11, 3, 100))
        spec[0] = np.array([[4000.0], [5500.0], [7000.0]]) + np.arange(100)[None, :] * 0.1 - 5.0
        spec[1, 1] = 600.0
        assert U.star_count_rate(spec, 60.0) == pytest.approx(10.0)
        assert math.isnan(U.star_count_rate(spec, 0.0))


class TestWiring:
    def test_the_moon_model_is_no_longer_forced(self, src):
        assert "here_moon = True" not in src

    def test_moon_corr_still_forces_it_per_frame(self, src):
        assert "if fsim.split('/')[-1] in spec_moon:" in src
        assert "here_moon = bool(use_moon[I][0])" in src

    def test_the_3_sigma_rule_is_gone(self, src):
        assert 'moon_contaminates' not in src
        assert not hasattr(U, 'MOON_NSIGMA')

    def test_the_moonlight_is_predicted_after_the_single_gaussian(self, src):
        single = src.index("moonv=refvel, moons=ferosutils_fp.MOON_SIGMA, moon=False)")
        pred = src.index("ferosutils_fp.predict_moon(mjd, moon_ra, moon_dec, star_rate, vels, p1gau, refvel)")
        assert single < pred

    def test_the_fit_is_tried_only_through_the_gate_or_moon_corr(self, src):
        assert ("moon_tried = moon_forced or ferosutils_fp.moon_can_bias(moon_pred['bias'], "
                "ferosutils_fp.ccf_rv_error(sp_type, p1gau, SNR_5130))") in src
        assert "moon_forced = know_moon and here_moon" in src

    def test_the_fit_is_bounded_and_kept_only_when_accepted(self, src):
        assert "moon=True, moon_bounded=True, moon_depth0=moon_pred['depth'])" in src
        assert "ferosutils_fp.accept_moon_fit(p1gau, fit_m[2], refvel, moon_pred['depth'])" in src
        keep = src.index("if keep_moon:")
        assert src.index("p1_m, XCmodel_m, p1gau_m, XCmodelgau_m, Ls2_m = fit_m", keep) > keep

    def test_an_rv_still_biased_by_moonlight_is_flagged(self, src):
        assert ("moon_unremoved = bool(moon_tried and (not moon_flag) and "
                "ferosutils_fp.moon_flags_rv(moon_pred['bias'], RVerr2))") in src
        assert "rv_good = rv_in_grid and (not moon_unremoved)" in src
        assert "'HIERARCH GOOD QUALITY RV', rv_good)" in src

    def test_the_prediction_and_the_fit_are_recorded(self, src):
        for card in ('MOON FIT', 'MOON SEP', 'MOON SKY', 'MOON RATIO', 'MOON BIAS',
                     'MOON DEPTH PRED', 'MOON DEPTH FIT', 'MOON SIGMA FIT', 'MOON REJECT'):
            assert f"'HIERARCH CERES {card}'" in src
