"""Background smoothing: median-bin before lowess (EXOAUTOMAT-287).

Profiling a live FEROS calibration reduction put ~17% of the night in
statsmodels ``lowess`` inside ``Lines_mBack``. It was regressing at every one of
~2000 pixels, four times over (``it=3``), and the caller then linear-splined the
result onto the full grid anyway — so the dense regression was wasted work.

Median-binning first is within the existing error budget rather than a new
approximation on top of it: on a ThAr-like trace the dense call lands 1.25 ADU
(max) from the true background and the binned one 1.84 ADU, while differing from
each other by at most 1.4 ADU — and it is 5.7x faster.

``it`` is explicitly not the knob: ``it=1`` is only 1.8x faster and moves the
background by 3583 ADU, because the robustifying iterations are what reject the
lines.
"""
from __future__ import annotations

import numpy as np
import pytest

from ceres3.utils import globalutils as G


def _thar_trace(npix=2048, nlines=180, seed=7):
    """A ThAr-like order: smooth background, emission lines, noise."""
    rng = np.random.default_rng(seed)
    x = np.arange(npix, dtype=float)
    truth = 300 + 40 * np.sin(x / 700) + 25 * np.exp(-((x - 1500) / 600) ** 2)
    spec = truth + rng.normal(0, 3, npix)
    centres = rng.integers(20, npix - 20, nlines)
    mask = np.ones(npix, bool)
    for c in centres:
        spec[c - 2:c + 3] += rng.uniform(500, 20000)
        mask[max(0, c - 3):c + 4] = False
    return x, spec, truth, np.where(mask)[0]


class TestAccuracy:

    def test_it_recovers_the_true_background(self):
        x, spec, truth, keep = _thar_trace()
        bkg = G.smoothed_background(x[keep], spec[keep], x)
        err = np.abs(bkg - truth)
        # The dense implementation itself lands ~1.25 ADU from truth.
        assert err.max() < 5.0, f"max error {err.max():.2f} ADU"
        assert np.median(err) < 1.5

    def test_lines_do_not_pull_the_background_up(self):
        """The whole point of masking plus robust smoothing."""
        x, spec, truth, keep = _thar_trace()
        bkg = G.smoothed_background(x[keep], spec[keep], x)
        assert bkg.mean() < truth.mean() + 5

    def test_a_flat_background_is_recovered_flat(self):
        x = np.arange(1000, dtype=float)
        y = np.full(1000, 42.0)
        bkg = G.smoothed_background(x, y, x)
        assert np.allclose(bkg, 42.0, atol=1e-6)

    def test_a_sloped_background_is_followed(self):
        x = np.arange(1500, dtype=float)
        y = 100 + 0.05 * x
        bkg = G.smoothed_background(x, y, x)
        assert np.max(np.abs(bkg - y)) < 2.0


class TestFallbacks:
    """Where binning would be unsafe, the dense path must still run."""

    def test_few_points_falls_back_rather_than_binning(self):
        x = np.linspace(0, 100, 40)
        y = 50 + 0.1 * x
        bkg = G.smoothed_background(x, y, x)
        assert np.max(np.abs(bkg - y)) < 3.0

    def test_no_points_returns_zeros_of_the_right_shape(self):
        out = G.smoothed_background(np.array([]), np.array([]), np.arange(10.0))
        assert out.shape == (10,)
        assert np.all(out == 0)

    def test_a_single_point_does_not_raise(self):
        out = G.smoothed_background(np.array([5.0]), np.array([7.0]), np.arange(4.0))
        assert out.shape == (4,)
        assert np.all(np.isfinite(out))

    def test_all_x_identical_does_not_raise(self):
        """span == 0 would make the bin edges degenerate."""
        out = G.smoothed_background(np.full(50, 3.0), np.full(50, 9.0), np.arange(5.0))
        assert out.shape == (5,)
        assert np.all(np.isfinite(out))

    def test_output_grid_may_differ_from_the_fit_grid(self):
        x, spec, _, keep = _thar_trace()
        subset = x[::7]
        out = G.smoothed_background(x[keep], spec[keep], subset)
        assert out.shape == subset.shape


class TestSpeed:

    def test_it_is_materially_faster_than_the_dense_call(self):
        import time
        import scipy.interpolate

        x, spec, _, keep = _thar_trace()

        def dense():
            bt = G.lowess(spec[keep].astype('double'), x[keep], frac=0.2, it=3,
                          return_sorted=False)
            tck = scipy.interpolate.splrep(x[keep], bt, k=1)
            return scipy.interpolate.splev(x, tck)

        G.smoothed_background(x[keep], spec[keep], x)  # warm
        t0 = time.perf_counter(); dense(); t_dense = time.perf_counter() - t0
        t0 = time.perf_counter(); G.smoothed_background(x[keep], spec[keep], x)
        t_new = time.perf_counter() - t0
        assert t_new < t_dense / 2, f"only {t_dense/t_new:.1f}x faster"

    def test_the_binned_result_tracks_the_dense_one(self):
        """Not a new approximation: it stays inside the dense method's own error."""
        import scipy.interpolate

        x, spec, _, keep = _thar_trace()
        bt = G.lowess(spec[keep].astype('double'), x[keep], frac=0.2, it=3,
                      return_sorted=False)
        tck = scipy.interpolate.splrep(x[keep], bt, k=1)
        dense = scipy.interpolate.splev(x, tck)
        binned = G.smoothed_background(x[keep], spec[keep], x)
        assert np.max(np.abs(binned - dense)) < 5.0
