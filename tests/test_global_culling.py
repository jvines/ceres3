"""Culling contract of the global wavelength-solution fits (ceres3 1.2, EXOAUTOMAT-268).

``Fit_Global_Wav_Solution`` and ``Global_Wav_Solution_vel_shift`` reject lines
beyond 4 sigma. Up to 1.1.5 both skipped the culling entirely when a line list
*started* below ``minlines``. FEROS asks for 1200 lines, so when the ThAr lamp
lost its thorium output (840-1170 identified lines instead of ~1650) every
misidentified line, with residuals of 13-805 km/s, stayed in the fit, and the
global solution came out with an rms of 0.4-28 km/s instead of ~105 m/s. The
batch culling also stopped as soon as a batch crossed ``minlines``, without
refitting, so the returned ``p1`` still contained the lines it reported as
rejected: one July ThAr kept lines that its own ``p1`` fit to 48 km/s rms.

1.2 keeps the legacy semantics by default, apart from those two fixes (refit
after the last removal, never cross the floor), and lets a caller opt in to
culling short lists with ``cull_floor``/``max_cull_frac``.

The line lists here are FEROS-like: the module's own Chebyshev basis with
FEROS's settings (Cheby, Inv, nx=5, nm=7, order0=26, ntotal=25, npix=4096),
a real FEROS global solution as the truth, 100 m/s Gaussian centroid noise and
a fraction of gross misidentifications.
"""
from __future__ import annotations

import numpy as np
import pytest
import scipy.optimize

from ceres3.utils import globalutils as G

C = 299792458.0
OO0, O0, NU, NX, NM, NPIX = 26, 8, 25, 5, 7, 4096
KW = dict(Cheby=True, Inv=True, order0=OO0, ntotal=NU, npix=NPIX, nx=NX, nm=NM)
INFO_KEYS = {"n_initial", "n_final", "rms_initial", "rms_final", "floor",
             "floor_hit", "n_removed", "iterations", "below_minlines"}

# Global object-fibre solution of FEROS ThAr 2026-09-13T17:16:43 (33 terms).
P_TRUE = np.array([
    2.264759e+05, 4.183558e+03, -1.431115e+02, -1.072725e+01, 2.972140e-01,
    -1.686001e-01, -8.130600e+01, -4.376223e+00, -1.651777e+01, -5.937148e+00,
    -3.071017e+00, -7.318387e-01, -1.315870e-01, -6.519201e+01, 1.434099e+01,
    -3.468874e+00, -3.021053e-01, -1.016438e-01, -3.543462e+01, 7.728364e+00,
    -2.368255e+00, -1.564004e-01, -4.254401e-02, -2.068948e+01, 3.979180e+00,
    -9.266626e-01, -8.052448e-02, -8.785087e+00, 1.227977e+00, -2.191350e-01,
    -2.384801e+00, 1.531104e-01, -4.041714e-01,
])


def model(p, pix, orders):
    m = orders + OO0
    chebs = G.Calculate_chebs(pix, m, order0=OO0, ntotal=NU, npix=NPIX,
                              Inverse=True, nx=NX, nm=NM)
    return (1.0 / m) * G.Joint_Polynomial_Cheby(p, chebs, NX, NM)


def make_lines(n_lines, bad_frac, seed, noise_ms=100.0, shift_ms=0.0):
    """FEROS-like line list; returns (pix, wav, orders, indices of gross misidentifications)."""
    rng = np.random.default_rng(seed)
    orders = rng.integers(O0, O0 + NU, n_lines).astype(float)
    pix = rng.uniform(55.0, 4000.0, n_lines)
    wav = model(P_TRUE, pix, orders) * (1.0 + (shift_ms + rng.normal(0.0, noise_ms, n_lines)) / C)
    n_bad = int(round(bad_frac * n_lines))
    bad = np.sort(rng.choice(n_lines, n_bad, replace=False))
    dv = np.exp(rng.uniform(np.log(13e3), np.log(805e3), n_bad)) * rng.choice([-1.0, 1.0], n_bad)
    wav[bad] *= 1.0 + dv / C
    return pix, wav, orders, bad


def p0():
    p = np.zeros(len(P_TRUE))
    p[0] = P_TRUE[0]
    return p


def fit_global(lines, minlines=1200, maxrms=150.0, **kw):
    pix, wav, orders, _ = lines
    info = {}
    p1, _, _, _, I, rms, res = G.Fit_Global_Wav_Solution(
        pix, wav, orders, np.ones(len(wav)), p0(), minlines=minlines, maxrms=maxrms,
        info=info, **KW, **kw)
    return p1, np.asarray(I), rms, res, info


def fit_shift(lines, minlines=1200, maxrms=150.0, **kw):
    pix, wav, orders, _ = lines
    info = {}
    p1, _, _, _, I, rms, res = G.Global_Wav_Solution_vel_shift(
        pix, wav, orders, np.ones(len(wav)), P_TRUE, minlines=minlines, maxrms=maxrms,
        info=info, **KW, **kw)
    return p1, np.asarray(I), rms, res, info


def removed(lines, I):
    return np.setdiff1d(np.arange(len(lines[1])), I)


def assert_self_consistent(lines, p1, I, rms, res, shift=False):
    """The returned residuals and rms are those of the returned p1 on the returned lines."""
    pix, wav, orders, _ = lines
    fit = model(P_TRUE, pix[I], orders[I]) * (1 + 1e-6 * p1[0]) if shift else model(p1, pix[I], orders[I])
    assert len(res) == len(I)
    np.testing.assert_allclose(res, fit - wav[I], rtol=0, atol=1e-9)
    np.testing.assert_allclose(rms, np.std(C * res / wav[I]), rtol=1e-12)


# ceres3 1.1.5 as shipped (Cheby branch), the reference for "legacy unchanged".

def _fit_global_v115(pix_centers, wavelengths, orders, Wgt, p0, minlines, maxrms):
    fitfunc_cheb = lambda p, chebs, m: (1.0 / m) * G.Joint_Polynomial_Cheby(p, chebs, nx=NX, nm=NM)
    errfunc_cheb = lambda p, chebs, y, m, w: np.ravel(w * (fitfunc_cheb(p, chebs, m) - y))
    errfunc_cheb_nw = lambda p, chebs, y, m: np.ravel(fitfunc_cheb(p, chebs, m) - y)
    chebs = G.Calculate_chebs(pix_centers, orders + OO0, Inverse=True, order0=OO0, ntotal=NU, npix=NPIX, nx=NX, nm=NM)
    p1, _ = scipy.optimize.leastsq(errfunc_cheb, p0, args=(chebs, wavelengths, orders + OO0, Wgt))
    residuals = errfunc_cheb_nw(p1, chebs, wavelengths, orders + OO0)
    residuals_ms = C * residuals / wavelengths
    rms_ms = np.sqrt(np.var(residuals_ms))
    N_l = len(pix_centers)
    I = list(range(N_l))
    cond = 1
    L = np.where(np.absolute(residuals_ms) > 4.0 * rms_ms)
    if ((len(L[0]) == 0) and (rms_ms < maxrms)) or (N_l < minlines):
        cond = 0
    while cond:
        outlier_indices = np.where(np.absolute(residuals_ms) > 4.0 * rms_ms)[0]
        if len(outlier_indices) == 0:
            if rms_ms < maxrms:
                cond = 0
            else:
                outlier_indices = np.where(np.absolute(residuals_ms) > 3.0 * rms_ms)[0]
                if len(outlier_indices) == 0:
                    cond = 0
        if cond and len(outlier_indices) > 0:
            for idx in sorted(outlier_indices, reverse=True):
                I.pop(idx)
            N_l = len(I)
            if N_l < minlines:
                cond = 0
                continue
            chebs = G.Calculate_chebs(pix_centers[I], orders[I] + OO0, Inverse=True, nx=NX, nm=NM, ntotal=NU, npix=NPIX, order0=OO0)
            p1, _ = scipy.optimize.leastsq(errfunc_cheb, p0, args=(chebs, wavelengths[I], orders[I] + OO0, Wgt[I]))
            residuals = errfunc_cheb_nw(p1, chebs, wavelengths[I], orders[I] + OO0)
            residuals_ms = C * residuals / wavelengths[I]
            rms_ms = np.sqrt(np.var(residuals_ms))
            L = np.where(np.absolute(residuals_ms) > 4. * rms_ms)
            if ((len(L[0]) == 0) and (rms_ms < maxrms)) or (N_l < minlines):
                cond = 0
    return p1, np.asarray(I), rms_ms, residuals


def _vel_shift_v115(pix_centers, wavelengths, orders, Wgt, p_ref, minlines, maxrms):
    fitfunc_cheb = lambda p, p_ref, chebs, m: ((1 + 1e-6 * p) / m) * G.Joint_Polynomial_Cheby(p_ref, chebs, nx=NX, nm=NM)
    errfunc_cheb = lambda p, p_ref, chebs, y, m, w: np.ravel(w * (fitfunc_cheb(p, p_ref, chebs, m) - y))
    errfunc_cheb_nw = lambda p, p_ref, chebs, y, m: np.ravel(fitfunc_cheb(p, p_ref, chebs, m) - y)
    p0 = np.array([0])
    chebs = G.Calculate_chebs(pix_centers, orders + OO0, order0=OO0, ntotal=NU, npix=NPIX, Inverse=True, nx=NX, nm=NM)
    p1, _ = scipy.optimize.leastsq(errfunc_cheb, p0, args=(p_ref, chebs, wavelengths, orders + OO0, Wgt))
    residuals = errfunc_cheb_nw(p1, p_ref, chebs, wavelengths, orders + OO0)
    residuals_ms = C * residuals / wavelengths
    rms_ms = np.sqrt(np.var(residuals_ms))
    N_l = len(pix_centers)
    I = list(range(N_l))
    cond = 1
    L = np.where(np.absolute(residuals_ms) > 4.0 * rms_ms)
    if ((len(L[0]) == 0) and (rms_ms < maxrms)) or (N_l < minlines):
        cond = 0
    while cond:
        index_worst = np.argmax(np.absolute(residuals))
        I.pop(index_worst)
        N_l -= 1
        chebs = G.Calculate_chebs(pix_centers[I], orders[I] + OO0, order0=OO0, ntotal=NU, npix=NPIX, Inverse=True, nx=NX, nm=NM)
        p1, _ = scipy.optimize.leastsq(errfunc_cheb, p0, args=(p_ref, chebs, wavelengths[I], orders[I] + OO0, Wgt[I]))
        residuals = errfunc_cheb_nw(p1, p_ref, chebs, wavelengths[I], orders[I] + OO0)
        residuals_ms = C * residuals / wavelengths[I]
        rms_ms = np.sqrt(np.var(residuals_ms))
        L = np.where(np.absolute(residuals_ms) > 4. * rms_ms)
        if ((len(L[0]) == 0) and (rms_ms < maxrms)) or (N_l < minlines):
            cond = 0
    return p1, np.asarray(I), rms_ms, residuals


@pytest.fixture(scope="module")
def healthy():
    """A healthy ThAr: 1650 lines, 5% misidentified."""
    return make_lines(1650, 0.05, seed=1)


@pytest.fixture(scope="module")
def degraded():
    """A ThAr whose lamp lost its thorium: 950 lines, 8% misidentified."""
    return make_lines(950, 0.08, seed=2)


@pytest.fixture(scope="module")
def at_floor():
    """1250 lines with 125 misidentified: culling them all would cross minlines=1200."""
    return make_lines(1250, 0.10, seed=3)


# ---------------------------------------------------------------- global fit

@pytest.mark.parametrize("maxrms", [150.0, 90.0])  # 90 < noise: the 3-sigma branch runs too
def test_legacy_mode_reproduces_v115_when_the_floor_is_not_reached(healthy, maxrms):
    pix, wav, orders, _ = healthy
    p1, I, rms, res, info = fit_global(healthy, maxrms=maxrms)
    p1_old, I_old, rms_old, res_old = _fit_global_v115(pix, wav, orders, np.ones(len(wav)), p0(), 1200, maxrms)

    assert info["n_final"] > 1200 and not info["floor_hit"]
    np.testing.assert_array_equal(I, I_old)
    np.testing.assert_array_equal(p1, p1_old)
    assert rms == rms_old
    np.testing.assert_array_equal(res, res_old)


def test_legacy_mode_does_not_cull_a_list_that_starts_short(degraded):
    pix, wav, orders, _ = degraded
    p1, I, rms, res, info = fit_global(degraded)
    p1_old, I_old, rms_old, _ = _fit_global_v115(pix, wav, orders, np.ones(len(wav)), p0(), 1200, 150.0)

    np.testing.assert_array_equal(I, np.arange(len(wav)))
    np.testing.assert_array_equal(p1, p1_old)
    assert rms == rms_old
    assert rms > 10e3  # every misidentification is still in: km/s-level solution
    assert info["floor_hit"] and info["below_minlines"]
    assert info["iterations"] == 0 and info["n_removed"] == 0


def test_opt_in_culls_the_misidentifications_of_a_short_list(degraded):
    pix, wav, orders, bad = degraded
    p1, I, rms, res, info = fit_global(degraded, cull_floor=300, max_cull_frac=0.5)

    gone = removed(degraded, I)
    assert set(bad) <= set(gone)
    assert len(gone) - len(bad) < 0.02 * (len(wav) - len(bad))  # few good lines lost
    assert rms < 115.0  # the 100 m/s centroid noise, not km/s
    grid_x = np.tile(np.arange(64.0, 4000.0, 64.0), NU)
    grid_o = np.repeat(np.arange(O0, O0 + NU, dtype=float), len(np.arange(64.0, 4000.0, 64.0)))
    dv = C * (model(p1, grid_x, grid_o) / model(P_TRUE, grid_x, grid_o) - 1.0)
    assert np.median(np.abs(dv)) < 30.0  # ~100 m/s * sqrt(33 terms / 874 lines)
    assert not info["floor_hit"] and info["below_minlines"]
    assert_self_consistent(degraded, p1, I, rms, res)


def test_opt_in_matches_legacy_on_a_healthy_list(healthy):
    p1, I, rms, _, info = fit_global(healthy, maxrms=90.0)
    p1_o, I_o, rms_o, _, info_o = fit_global(healthy, maxrms=90.0, cull_floor=300, max_cull_frac=0.5)

    np.testing.assert_array_equal(I_o, I)
    np.testing.assert_array_equal(p1_o, p1)
    assert rms_o == rms
    assert info_o["floor"] == 825 and info["floor"] == 1200


def test_legacy_batch_stops_at_minlines_and_refits(at_floor):
    pix, wav, orders, bad = at_floor
    p1, I, rms, res, info = fit_global(at_floor)

    assert len(I) == 1200  # not below it, as 1.1.5 went
    assert info["floor_hit"] and info["floor"] == 1200 and not info["below_minlines"]
    assert set(removed(at_floor, I)) <= set(bad)  # only the worst lines went
    assert_self_consistent(at_floor, p1, I, rms, res)

    # 1.1.5 on the same list: the last batch dropped below minlines and left
    # p1 fit to the lines it had just rejected.
    p1_old, I_old, rms_old, res_old = _fit_global_v115(pix, wav, orders, np.ones(len(wav)), p0(), 1200, 150.0)
    assert len(I_old) < 1200
    assert len(res_old) != len(I_old)
    true_old = np.std(C * (model(p1_old, pix[I_old], orders[I_old]) - wav[I_old]) / wav[I_old])
    assert abs(true_old - rms_old) > 1.0


def test_max_cull_frac_raises_the_floor(degraded):
    bad = degraded[3]
    p1, I, rms, res, info = fit_global(degraded, cull_floor=300, max_cull_frac=0.05)

    assert info["floor"] == 903  # ceil(950 * 0.95)
    assert len(I) == 903 and info["floor_hit"]
    assert set(removed(degraded, I)) <= set(bad)
    assert_self_consistent(degraded, p1, I, rms, res)


def test_cull_floor_blocks_culling_of_a_list_already_below_it():
    lines = make_lines(250, 0.08, seed=4)
    p1, I, rms, res, info = fit_global(lines, cull_floor=300)

    assert len(I) == 250 and info["floor_hit"] and info["iterations"] == 0


def test_info_is_filled(degraded):
    info = fit_global(degraded, cull_floor=300, max_cull_frac=0.5)[-1]

    assert set(info) == INFO_KEYS
    assert info["n_initial"] == 950 and info["floor"] == 475
    assert info["n_removed"] == info["n_initial"] - info["n_final"] > 0
    assert info["iterations"] >= 1
    assert info["rms_initial"] > 10e3 > 115.0 > info["rms_final"]
    assert info["below_minlines"] is (info["n_final"] < 1200)
    for key, typ in (("n_initial", int), ("n_final", int), ("floor", int), ("n_removed", int),
                     ("iterations", int), ("rms_initial", float), ("rms_final", float),
                     ("floor_hit", bool), ("below_minlines", bool)):
        assert type(info[key]) is typ, key


def test_info_is_optional(healthy):
    pix, wav, orders, _ = healthy
    out = G.Fit_Global_Wav_Solution(pix, wav, orders, np.ones(len(wav)), p0(), minlines=1200, **KW)
    assert len(out) == 7


@pytest.mark.parametrize("n, minlines, cull_floor, frac, expected", [
    (950, 1200, None, None, 1200),
    (950, 1200, 300, None, 300),
    (950, 1200, 300, 0.5, 475),
    (500, 1200, 300, 0.5, 300),
    (1651, 1000, 300, 0.5, 826),
    (1000, 1000, 300, 0.3, 700),
])
def test_culling_floor(n, minlines, cull_floor, frac, expected):
    assert G._culling_floor(n, minlines, cull_floor, frac) == expected


@pytest.mark.parametrize("cull_floor, frac", [(None, 0.5), (300, -0.1), (300, 1.5)])
def test_culling_floor_rejects_bad_arguments(cull_floor, frac):
    with pytest.raises(ValueError):
        G._culling_floor(1000, 1200, cull_floor, frac)


# --------------------------------------------------------------- velocity shift

@pytest.fixture(scope="module")
def healthy_shifted():
    return make_lines(1650, 0.05, seed=5, shift_ms=150.0)


@pytest.fixture(scope="module")
def degraded_shifted():
    return make_lines(950, 0.08, seed=6, shift_ms=150.0)


@pytest.mark.parametrize("maxrms", [150.0, 90.0])
def test_vel_shift_legacy_mode_reproduces_v115(healthy_shifted, maxrms):
    pix, wav, orders, _ = healthy_shifted
    p1, I, rms, res, info = fit_shift(healthy_shifted, maxrms=maxrms)
    p1_old, I_old, rms_old, res_old = _vel_shift_v115(pix, wav, orders, np.ones(len(wav)), P_TRUE, 1200, maxrms)

    assert info["n_final"] > 1200 and not info["floor_hit"]
    np.testing.assert_array_equal(I, I_old)
    np.testing.assert_array_equal(p1, p1_old)
    assert rms == rms_old
    assert abs(1e-6 * p1[0] * C - 150.0) < 10.0


def test_vel_shift_opt_in_culls_a_short_list(degraded_shifted):
    bad = degraded_shifted[3]
    p1_leg, I_leg, _, _, info_leg = fit_shift(degraded_shifted)
    p1, I, rms, res, info = fit_shift(degraded_shifted, cull_floor=300, max_cull_frac=0.5)

    assert len(I_leg) == 950 and info_leg["floor_hit"] and info_leg["iterations"] == 0
    assert set(bad) <= set(removed(degraded_shifted, I))
    assert rms < 115.0 and not info["floor_hit"]
    assert abs(1e-6 * p1[0] * C - 150.0) < 20.0  # ~3.4 m/s precision
    assert_self_consistent(degraded_shifted, p1, I, rms, res, shift=True)
    assert set(info) == INFO_KEYS


def test_vel_shift_stops_at_the_floor(at_floor):
    pix, wav, orders, bad = at_floor
    p1, I, rms, res, info = fit_shift(at_floor)

    assert len(I) == 1200 and info["floor_hit"] and not info["below_minlines"]
    assert set(removed(at_floor, I)) <= set(bad)
    assert_self_consistent(at_floor, p1, I, rms, res, shift=True)
    # 1.1.5 removed one line past minlines
    assert len(_vel_shift_v115(pix, wav, orders, np.ones(len(wav)), P_TRUE, 1200, 150.0)[1]) == 1199
