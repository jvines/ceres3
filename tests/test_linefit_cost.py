"""The line fit is numpy-overhead-bound, so avoidable work matters (EXOAUTOMAT-287).

py-spy put ~33% of a FEROS night in ``IntGaussian`` inside the per-line
``leastsq`` residual. The windows are ~20 pixels, so each model evaluation is
about eight numpy operations on tiny arrays, forty to eighty evaluations per fit,
~1500 lines per frame — the cost is per-call overhead, not arithmetic.

Two things were pure overhead and both are bit-identical to remove:

* ``sqrt(2)`` was recomputed on every ``IntGaussian`` call (10.35 -> 8.77 us).
* ``fitfunc`` allocated a zero array and accumulated into it even for a single
  component, which is the overwhelmingly common case.

Measured together: ``LineFit_SingleSigma`` 383.3 -> 315.4 us per fit (17.7%),
with the fitted centroid identical to ten decimal places.

An analytic Jacobian was tried and rejected twice. In pure Python it was 0.98x --
building it costs what it saves in function evaluations. Compiled with numba it
was genuinely fast (355 -> 51 us per fit, 6.9x), but it replaces
scipy.special.erf (Cephes) with math.erf (libm), and those differ in the last
bit. Re-reducing a real night, that perturbation changed which lines survived
culling and moved the reported ThAr drift by 1.2 m/s (scatter, max 4.8) against
a 2.3-2.9 m/s precision floor. It was removed; the remaining changes here are
bit-identical.
"""
from __future__ import annotations

import numpy as np
import pytest
from scipy import special

from ceres3.utils import globalutils as G


def _reference_int_gaussian(x, mu, sigma):
    """The original formulation, recomputing sqrt(2) each call."""
    s2 = np.sqrt(2)
    arg1 = (x + 0.5 - mu) / (s2 * sigma)
    arg2 = (x - 0.5 - mu) / (s2 * sigma)
    return 0.5 * (special.erf(arg1) - special.erf(arg2))


class TestIntGaussianUnchanged:

    @pytest.mark.parametrize("mu, sigma", [(1010.3, 2.2), (0.0, 1.0),
                                           (-5.5, 0.4), (2048.0, 8.0)])
    def test_bit_identical_to_the_original(self, mu, sigma):
        x = np.arange(mu - 10, mu + 11, dtype=float)
        assert np.array_equal(G.IntGaussian(x, mu, sigma),
                              _reference_int_gaussian(x, mu, sigma))

    def test_the_hoisted_constant_is_sqrt_two(self):
        assert G._SQRT2 == np.sqrt(2)

    def test_it_still_integrates_to_one_over_a_wide_window(self):
        """A pixel-integrated Gaussian must conserve flux."""
        x = np.arange(-60, 61, dtype=float)
        assert np.isclose(G.IntGaussian(x, 0.0, 3.0).sum(), 1.0, atol=1e-9)

    def test_a_narrow_line_lands_in_one_pixel(self):
        x = np.arange(-5, 6, dtype=float)
        got = G.IntGaussian(x, 0.0, 0.05)
        assert np.isclose(got[5], 1.0, atol=1e-6)
        assert got.sum() == pytest.approx(1.0, abs=1e-9)


class TestSingleComponentPath:
    """n == 1 skips the zero allocation; adding to zeros is exact, so results
    must not move."""

    def _fit(self, n):
        rng = np.random.default_rng(11)
        width = 21 + 10 * (n - 1)
        x = np.arange(1000, 1000 + width, dtype=float)
        mu = np.array([1010.0 + 8 * i for i in range(n)])
        sigma = np.zeros(n) + 2.2
        y = sum(5000 * G.IntGaussian(x, m, 2.2) for m in mu) + rng.normal(0, 5, width)
        return G.LineFit_SingleSigma(x, y, np.zeros(width), mu, sigma, np.ones(width))

    def test_a_single_line_fits_its_centroid(self):
        """p_output is 3n long: [intensity, centre, sigma] per line."""
        p = self._fit(1)
        assert abs(p[0][1] - 1010.0) < 0.5
        assert abs(p[0][2] - 2.2) < 0.5          # shared sigma

    @pytest.mark.parametrize("n", [2, 3])
    def test_blends_still_fit_every_component(self, n):
        """The multi-component branch must not have been broken by the fast path."""
        p = self._fit(n)
        for i in range(n):
            assert abs(p[0][i * 3 + 1] - (1010.0 + 8 * i)) < 1.0

    def test_single_and_loop_paths_agree(self):
        """Evaluate the model both ways and require exact equality."""
        x = np.arange(1000, 1021, dtype=float)
        p = np.array([2.2, 5000.0, 1010.0])
        single = p[1] * G.IntGaussian(x, p[2], p[0])
        looped = np.zeros(len(x))
        for i in range(1):
            looped += p[i * 2 + 1] * G.IntGaussian(x, p[i * 2 + 2], p[0])
        assert np.array_equal(single, looped)


class TestSpeed:

    def test_the_fit_is_faster_than_the_original_formulation(self):
        import time

        rng = np.random.default_rng(3)
        width = 21
        x = np.arange(1000, 1000 + width, dtype=float)
        y = 5000 * G.IntGaussian(x, 1010.0, 2.2) + rng.normal(0, 5, width)
        args = (x, y, np.zeros(width), np.array([1010.0]), np.array([2.2]), np.ones(width))
        G.LineFit_SingleSigma(*args)

        t0 = time.perf_counter()
        for _ in range(200):
            G.LineFit_SingleSigma(*args)
        per_fit = (time.perf_counter() - t0) / 200
        # The original measured 383 us on this machine class; allow generous room
        # for slower hardware while still catching a regression to the old cost.
        assert per_fit < 2e-3, f"{per_fit*1e6:.0f} us/fit"
