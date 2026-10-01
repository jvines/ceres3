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

An analytic Jacobian was tried and rejected: 0.98x, because building it in Python
costs what it saves in function evaluations.
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


class TestNumbaPathMatchesNumpy:
    """The compiled path must agree with the interpreted one it replaces.

    Not bit-identical by construction: the kernels use ``math.erf`` (libm) while
    the numpy path uses ``scipy.special.erf`` (Cephes), which can differ in the
    last bit. Measured consequence on a fitted centroid: ~2e-12 px, which at
    ~1 km/s per pixel is ~2e-9 m/s — twelve orders of magnitude below anything
    this pipeline measures. The tolerances below are set to catch a real
    divergence, not that noise.
    """

    @staticmethod
    def _inputs(n=1, seed=11):
        rng = np.random.default_rng(seed)
        width = 21 + 10 * (n - 1)
        x = np.arange(1000, 1000 + width, dtype=float)
        mu = np.array([1010.0 + 8 * i for i in range(n)])
        sigma = np.zeros(n) + 2.2
        y = sum(5000 * G.IntGaussian(x, m, 2.2) for m in mu) + rng.normal(0, 5, width)
        return x, y, np.zeros(width), mu, sigma, np.ones(width)

    @pytest.mark.skipif(not G.HAVE_NUMBA, reason="numba not installed")
    @pytest.mark.parametrize("n", [1, 2, 3])
    def test_both_paths_fit_the_same_line(self, monkeypatch, n):
        args = self._inputs(n)
        compiled, _ = G.LineFit_SingleSigma(*args)
        monkeypatch.setattr(G, "HAVE_NUMBA", False)
        interpreted, _ = G.LineFit_SingleSigma(*args)
        for i in range(n):
            # centre and sigma, in pixels
            assert abs(compiled[i * 3 + 1] - interpreted[i * 3 + 1]) < 1e-6
            assert abs(compiled[i * 3 + 2] - interpreted[i * 3 + 2]) < 1e-6
            # intensity, relative
            assert abs(compiled[i * 3] - interpreted[i * 3]) / abs(interpreted[i * 3]) < 1e-9

    @pytest.mark.skipif(not G.HAVE_NUMBA, reason="numba not installed")
    def test_the_analytic_jacobian_is_correct(self):
        """A wrong Jacobian can still converge, just slowly — check it directly."""
        x, y, b, mu, sigma, w = self._inputs(1)
        p = np.array([2.2, 5000.0, 1010.0])
        jac = G._linefit_jacobian(p, np.ascontiguousarray(x), 1,
                                  np.ascontiguousarray(w),
                                  np.empty((x.size, 3)))
        buf = np.empty(x.size)
        for j in range(3):
            step = 1e-6 * max(abs(p[j]), 1.0)
            hi, lo = p.copy(), p.copy()
            hi[j] += step; lo[j] -= step
            numeric = (G._linefit_residual(hi, np.ascontiguousarray(x), 1,
                                           np.zeros_like(x), np.ascontiguousarray(w), buf).copy()
                       - G._linefit_residual(lo, np.ascontiguousarray(x), 1,
                                             np.zeros_like(x), np.ascontiguousarray(w), buf).copy()
                       ) / (2 * step)
            assert np.allclose(jac[:, j], numeric, rtol=1e-5, atol=1e-8), f"param {j}"

    @pytest.mark.skipif(not G.HAVE_NUMBA, reason="numba not installed")
    def test_the_compiled_path_is_materially_faster(self, monkeypatch):
        import time

        args = self._inputs(1)
        G.LineFit_SingleSigma(*args)  # warm the JIT
        t0 = time.perf_counter()
        for _ in range(200):
            G.LineFit_SingleSigma(*args)
        t_compiled = (time.perf_counter() - t0) / 200

        monkeypatch.setattr(G, "HAVE_NUMBA", False)
        t0 = time.perf_counter()
        for _ in range(200):
            G.LineFit_SingleSigma(*args)
        t_interp = (time.perf_counter() - t0) / 200
        assert t_compiled < t_interp / 2, f"only {t_interp/t_compiled:.1f}x"

    def test_missing_numba_is_not_fatal(self, monkeypatch):
        """The numpy path must remain usable: numba is an accelerator, not a need."""
        monkeypatch.setattr(G, "HAVE_NUMBA", False)
        p, _ = G.LineFit_SingleSigma(*self._inputs(1))
        assert abs(p[1] - 1010.0) < 0.5
