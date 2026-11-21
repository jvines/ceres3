"""Interpolated drift for ObjCal frames whose own drift fit failed (EXOAUTOMAT-242).

In ~26% of tau Ceti frames (all simultaneous-ThAr, ObjCal) the simultaneous-fibre
drift fit misses the 5 m/s precision cut and the frame used to be left with
INSTRUMENTAL DRIFT = 0, i.e. uncorrected by ~86 m/s typical. Such a frame now
takes the drift interpolated from the night's measured drifts when the nearest
measured one is within ``DRIFT_INTERP_MAX_GAP_MIN``, and records the provenance in
``DRIFT SOURCE`` / ``DRIFT ANCHOR GAP`` / ``DRIFT NANCHORS``.

The numerical half exercises the helpers in ``ferosutils_fp``; the structural half
checks how ``ferospipe_fp`` (a module-scope script with no importable seam) wires
them, so that measured frames keep their own drift untouched.
"""
from __future__ import annotations

import ast
from pathlib import Path

import numpy as np
import pytest
import scipy.interpolate

from ceres3.instruments import ferosutils_fp as fu

PIPE = (
    Path(__file__).resolve().parents[1]
    / "src" / "ceres3" / "instruments" / "ferospipe_fp.py"
)
C = 299792458.0
MIN = 1.0 / 1440.0   # one minute in days


def _to_pshift(v_ms):
    return 1e6 * np.asarray(v_ms, dtype=float) / C


def _to_ms(p_shift):
    return C * np.asarray(p_shift, dtype=float) / 1e6


# --------------------------------------------------------------------------- helpers

class TestInterpolateFailedDrift:
    t0 = 60000.0

    def test_between_anchors_matches_objsky_spline(self):
        t = self.t0 + np.array([0., 20., 45., 70.]) * MIN
        s = _to_pshift([80., 95., 60., 110.])
        tck = scipy.interpolate.splrep(t, s, k=1)
        for m in self.t0 + np.array([5., 20., 33.3, 69.]) * MIN:
            got, src, _ = fu.interpolate_failed_drift(m, t, s)
            assert src == fu.DRIFT_SOURCE_INTERPOLATED
            assert got == pytest.approx(float(scipy.interpolate.splev(m, tck)), rel=1e-12, abs=1e-15)

    def test_sign_recovers_a_dropped_measurement(self):
        # A linear drift track with both signs: dropping any interior frame and
        # interpolating from its neighbours gives back its own drift, sign and all.
        t = self.t0 + np.arange(0., 100., 10.) * MIN
        v = -120. + 3.0 * np.arange(0., 100., 10.)   # -120 .. +150 m/s
        s = _to_pshift(v)
        for i in range(1, len(t) - 1):
            keep = np.arange(len(t)) != i
            got, src, gap = fu.interpolate_failed_drift(t[i], t[keep], s[keep])
            assert src == fu.DRIFT_SOURCE_INTERPOLATED
            assert gap == pytest.approx(10.0)
            assert float(_to_ms(got)) == pytest.approx(v[i], abs=1e-6)
            assert np.sign(_to_ms(got)) == np.sign(v[i]) or v[i] == 0

    def test_applied_like_a_measured_drift(self):
        # The pipeline applies a measured drift as lambda *= (1 + 1e-6 p_shift); the
        # interpolated one must enter with the same sign convention, so a frame that
        # gets exactly its neighbours' drift ends up on their wavelength scale.
        lam = np.linspace(3500., 9200., 7)
        p = _to_pshift(86.)
        t = self.t0 + np.array([0., 10.]) * MIN
        got, _, _ = fu.interpolate_failed_drift(self.t0 + 5 * MIN, t, [p, p])
        np.testing.assert_allclose(lam * (1.0 + 1.0e-6 * got), lam * (1.0 + 1.0e-6 * p), rtol=0, atol=0)
        assert got > 0

    def test_no_anchor(self):
        got, src, gap = fu.interpolate_failed_drift(self.t0, [], [])
        assert (got, src, gap) == (0., fu.DRIFT_SOURCE_NONE, None)

    def test_nan_anchors_are_ignored(self):
        got, src, gap = fu.interpolate_failed_drift(self.t0, [np.nan], [np.nan])
        assert (got, src, gap) == (0., fu.DRIFT_SOURCE_NONE, None)
        t = self.t0 + np.array([-5., 5.]) * MIN
        got, src, _ = fu.interpolate_failed_drift(self.t0, t, [np.nan, _to_pshift(50.)])
        assert src == fu.DRIFT_SOURCE_INTERPOLATED
        assert float(_to_ms(got)) == pytest.approx(50.)

    def test_single_anchor_is_held(self):
        s = _to_pshift(-73.)
        for dt in (-40., 0., 25.):
            got, src, gap = fu.interpolate_failed_drift(self.t0 + dt * MIN, [self.t0], [s])
            assert src == fu.DRIFT_SOURCE_INTERPOLATED
            assert got == pytest.approx(float(s))
            assert gap == pytest.approx(abs(dt))

    def test_outside_span_holds_nearest_not_extrapolates(self):
        t = self.t0 + np.array([0., 10.]) * MIN
        s = _to_pshift([50., 100.])
        got, _, gap = fu.interpolate_failed_drift(self.t0 + 30 * MIN, t, s)
        assert float(_to_ms(got)) == pytest.approx(100.)   # a k=1 spline would give 200
        assert gap == pytest.approx(20.)
        got, _, _ = fu.interpolate_failed_drift(self.t0 - 30 * MIN, t, s)
        assert float(_to_ms(got)) == pytest.approx(50.)

    def test_unsorted_anchors(self):
        t = self.t0 + np.array([20., 0., 10.]) * MIN
        s = _to_pshift([30., 10., 20.])
        got, _, _ = fu.interpolate_failed_drift(self.t0 + 15 * MIN, t, s)
        assert float(_to_ms(got)) == pytest.approx(25.)

    def test_gap_limit(self):
        s = _to_pshift(90.)
        lim = fu.DRIFT_INTERP_MAX_GAP_MIN
        got, src, gap = fu.interpolate_failed_drift(self.t0 + lim * MIN, [self.t0], [s])
        assert src == fu.DRIFT_SOURCE_INTERPOLATED and gap == pytest.approx(lim)
        got, src, gap = fu.interpolate_failed_drift(self.t0 + (lim + 0.5) * MIN, [self.t0], [s])
        assert (got, src) == (0., fu.DRIFT_SOURCE_NONE)
        assert gap == pytest.approx(lim + 0.5)

    def test_gap_is_to_the_nearest_anchor_on_either_side(self):
        # Between two anchors 3 h apart, 50 min from one: interpolated.
        t = self.t0 + np.array([0., 180.]) * MIN
        _, src, gap = fu.interpolate_failed_drift(self.t0 + 130 * MIN, t, _to_pshift([0., 90.]))
        assert src == fu.DRIFT_SOURCE_INTERPOLATED and gap == pytest.approx(50.)
        # Midway, 90 min from both: not.
        _, src, gap = fu.interpolate_failed_drift(self.t0 + 90 * MIN, t, _to_pshift([0., 90.]))
        assert src == fu.DRIFT_SOURCE_NONE and gap == pytest.approx(90.)

    def test_custom_gap_parameter(self):
        _, src, _ = fu.interpolate_failed_drift(self.t0 + 100 * MIN, [self.t0], [1.], max_gap_min=120.)
        assert src == fu.DRIFT_SOURCE_INTERPOLATED
        _, src, _ = fu.interpolate_failed_drift(self.t0 + 10 * MIN, [self.t0], [1.], max_gap_min=5.)
        assert src == fu.DRIFT_SOURCE_NONE
        _, src, _ = fu.interpolate_failed_drift(self.t0, [self.t0], [1.], max_gap_min=None)
        assert src == fu.DRIFT_SOURCE_NONE

    def test_default_gap_is_60_min(self):
        assert fu.DRIFT_INTERP_MAX_GAP_MIN == 60.0


class TestDriftSourceCards:
    def test_interpolated(self):
        cards = {k: v for k, v, _ in fu.drift_source_cards(fu.DRIFT_SOURCE_INTERPOLATED, 12.3456, 7)}
        assert cards == {
            'HIERARCH DRIFT SOURCE': 'INTERPOLATED',
            'HIERARCH DRIFT ANCHOR GAP': 12.35,
            'HIERARCH DRIFT NANCHORS': 7,
        }

    def test_none_without_anchor(self):
        cards = {k: v for k, v, _ in fu.drift_source_cards(fu.DRIFT_SOURCE_NONE, None, 0)}
        assert cards['HIERARCH DRIFT SOURCE'] == 'NONE'
        assert cards['HIERARCH DRIFT ANCHOR GAP'] == -1.0
        assert cards['HIERARCH DRIFT NANCHORS'] == 0

    def test_measured_omits_anchor_count(self):
        cards = {k: v for k, v, _ in fu.drift_source_cards(fu.DRIFT_SOURCE_MEASURED, 0.)}
        assert cards == {'HIERARCH DRIFT SOURCE': 'MEASURED', 'HIERARCH DRIFT ANCHOR GAP': 0.0}

    def test_cards_fit_a_fits_header(self):
        from astropy.io import fits
        h = fits.Header()
        for k, v, c in fu.drift_source_cards(fu.DRIFT_SOURCE_INTERPOLATED, 59.99, 12):
            h[k] = (v, c)
        assert h['DRIFT SOURCE'] == 'INTERPOLATED'
        h.tostring()


# --------------------------------------------------------------------- pipeline wiring

@pytest.fixture(scope="module")
def src() -> str:
    return PIPE.read_text()


@pytest.fixture(scope="module")
def tree(src) -> ast.Module:
    return ast.parse(src, filename=str(PIPE))


def _top_for(tree, name):
    return [n for n in tree.body if isinstance(n, ast.For)
            and isinstance(n.iter, ast.Name) and n.iter.id == name]


def test_failed_frames_queued_only_on_failure_and_for_wave(tree):
    loop = _top_for(tree, "comp_list")[0]
    queued = [n for n in ast.walk(loop) if isinstance(n, ast.If)
              and any(isinstance(s, ast.Expr) and ast.unparse(s).startswith("objcal_drift_failed.append")
                      for s in n.body)]
    assert len(queued) == 1
    test = ast.unparse(queued[0].test)
    assert "not drift_ok" in test and "comp_type == 'WAVE'" in test


def test_failed_frame_block_runs_after_anchors_and_before_objsky(tree):
    body = tree.body
    idx = {}
    for i, n in enumerate(body):
        u = ast.unparse(n)
        if isinstance(n, ast.For) and isinstance(n.iter, ast.Name) and n.iter.id == "objcal_drift_failed":
            idx["failed"] = i
        if isinstance(n, ast.For) and isinstance(n.iter, ast.Name) and n.iter.id == "new_sky":
            idx["sky"] = i
        if u.startswith("p_mjds, p_shifts = (p_mjds[I], p_shifts[I])") or u.startswith("p_mjds, p_shifts = p_mjds[I], p_shifts[I]"):
            idx["sorted"] = i
    assert idx["sorted"] < idx["failed"] < idx["sky"]
    block = ast.unparse(body[idx["failed"]])
    assert "interpolate_failed_drift(mjd_f, p_mjds, p_shifts, drift_max_gap)" in block
    # The drift multiplies the wavelength plane only when one was interpolated.
    guarded = [n for n in ast.walk(body[idx["failed"]]) if isinstance(n, ast.If)
               and "DRIFT_SOURCE_INTERPOLATED" in ast.unparse(n.test)]
    assert len(guarded) == 1
    def _data_scalings(root):
        return [n for n in ast.walk(root) if isinstance(n, ast.AugAssign)
                and ".data[" in ast.unparse(n.target)]
    assert [ast.unparse(n.target) for n in _data_scalings(guarded[0])] == ["hdu[0].data[0, :, :]"]
    assert _data_scalings(body[idx["failed"]]) == _data_scalings(guarded[0])


def test_measured_frames_are_not_revisited(tree):
    # Measured frames never enter the queue (see above), and nothing after the
    # science loop reopens a product other than the queued and ObjSky ones.
    opens = []
    for n in tree.body:
        if isinstance(n, ast.For) and isinstance(n.iter, ast.Name) and n.iter.id in ("objcal_drift_failed", "new_sky"):
            opens.append(n.iter.id)
    assert opens == ["objcal_drift_failed", "new_sky"]


def test_gap_is_a_cli_parameter(src):
    assert "'-drift_max_gap'" in src
    assert "default=ferosutils_fp.DRIFT_INTERP_MAX_GAP_MIN" in src
    assert "drift_max_gap = args.drift_max_gap" in src
