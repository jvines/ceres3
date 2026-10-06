"""Scattered moonlight in a fibre and the bias it puts on a single-Gaussian CCF RV (EXOAUTOMAT-301)."""
from __future__ import annotations

import math

import numpy as np
import pytest

from ceres3.utils import moonlight as M

V = np.arange(-30.0, 30.0, 0.2)


class TestSkyBrightness:
    def test_no_moonlight_with_the_moon_down(self):
        assert math.isinf(M.ks_sky_brightness(0.0, 30.0, 95.0, 30.0))
        assert math.isinf(M.ks_sky_brightness(0.0, 30.0, 90.0, 30.0))

    def test_a_full_moon_brightens_the_sky_to_about_18_mag(self):
        assert 17.0 < M.ks_sky_brightness(0.0, 30.0, 40.0, 30.0) < 19.0

    def test_brighter_nearer_the_moon_and_nearer_full(self):
        full_near, full_far = M.ks_sky_brightness(0.0, 20.0, 40.0, 30.0), M.ks_sky_brightness(0.0, 90.0, 40.0, 30.0)
        half_near = M.ks_sky_brightness(90.0, 20.0, 40.0, 30.0)
        assert full_near < full_far
        assert full_near < half_near


class TestRatio:
    def test_none_with_the_moon_down(self):
        assert M.moonlight_ratio(np.inf, 100.0, 12.2, np.pi) == 0.0

    def test_scales_with_the_star_s_brightness(self):
        bright = M.moonlight_ratio(18.0, 100.0, 12.2, np.pi)
        faint = M.moonlight_ratio(18.0, 1.0, 12.2, np.pi)          # 5 mag fainter
        assert faint == pytest.approx(100.0 * bright)

    def test_a_star_with_no_counts_has_no_usable_ratio(self):
        assert math.isinf(M.moonlight_ratio(18.0, 0.0, 12.2, np.pi))


class TestPredictedBias:
    def test_none_without_a_moon(self):
        assert M.predicted_bias(V, 0.33, 0.0, 4.0, 0.0, 6.0, 4.0) == 0.0

    def test_none_with_the_moon_on_the_star_or_far_from_it(self):
        assert M.predicted_bias(V, 0.33, 0.0, 4.0, 0.01, 0.0, 4.0) == pytest.approx(0.0, abs=1e-5)
        assert abs(M.predicted_bias(V, 0.33, 0.0, 4.0, 0.01, 26.0, 4.0)) < 1e-4

    @pytest.mark.parametrize("side", [-1, 1])
    def test_pulls_the_centre_toward_the_moon(self, side):
        assert side * M.predicted_bias(V, 0.33, 0.0, 4.0, 0.01, side * 6.0, 4.0) > 0

    def test_a_few_m_s_per_0_001_of_moonlight_at_1_to_2_sigma(self):
        # the archive-calibrated scale: a 1e-3 moon fraction under a G2 mask is a ~3.5e-4 dip
        b = M.predicted_bias(V, 0.33, 0.0, 4.0, 0.35e-3, 6.0, 4.0) * 1000
        assert 1.0 < b < 5.0


class TestGeometry:
    def test_tau_ceti_2020_12_13_had_the_moon_set(self):
        g = M.moon_geometry(59197.02182915393, 26.0066, -15.9325, -70.7346, -29.2543, 2335.0)
        assert g['moon_alt'] == pytest.approx(-19.4, abs=0.5)
        assert g['separation'] == pytest.approx(122.3, abs=0.5)
        assert (1 + math.cos(math.radians(g['phase_angle']))) / 2 == pytest.approx(0.007, abs=0.003)
