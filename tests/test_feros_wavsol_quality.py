"""Grading of FEROS ThAr wavelength solutions and the calibration assessment.

A ThAr global solution is 'good' when it keeps >= REF_MIN_LINES lines per fibre
at a true global RMS <= REF_MAX_RMS_MS with no cull-floor hit, 'degraded' when
only the RMS test passes, and 'bad' otherwise. The nightly reference is chosen
among the best grade present; science frames carry header cards that AND the
drift precision with the reference grade; ``assess_calibration`` summarises a
reduced calibration directory for ExoAutomata and must never raise.

Real-data checks use the production calibration directories of 2026-09-13
(healthy) and 2026-09-15 (weak red-end comparison traces missed, orders
misregistered) when ``CERES3_FEROS_CALIB_ROOT`` points at a directory holding
``<night>_red`` copies; they are skipped otherwise (the pickles are large and
are not committed).
"""
from __future__ import annotations

import json
import os
import pickle
import warnings
from pathlib import Path

import numpy as np
import pytest
from astropy.io import fits

from ceres3.instruments import ferosutils_fp as F
from ceres3.utils import globalutils as G

C = 299792458.0


# --------------------------------------------------------------------------
# grading
# --------------------------------------------------------------------------
@pytest.mark.parametrize(
    "n_ob, n_co, r_ob, r_co, fh, grade",
    [
        (1650, 1750, 105.0, 101.0, False, "good"),
        (1200, 1200, 150.0, 150.0, False, "good"),       # both limits inclusive
        (1199, 1750, 105.0, 101.0, False, "degraded"),   # lamp lost thorium
        (840, 1170, 123.0, 110.0, False, "degraded"),
        (1650, 1750, 105.0, 101.0, True, "degraded"),    # culling cut short by the floor
        (1650, 1750, 150.1, 101.0, False, "bad"),
        (720, 720, 26583.4, 6758.9, False, "bad"),       # 2026-09-15 reference
        (1650, 1750, float("nan"), 101.0, False, "bad"),
        (None, 1750, 105.0, 101.0, False, "bad"),
    ],
)
def test_grade_wavsol(n_ob, n_co, r_ob, r_co, fh, grade):
    assert F.grade_wavsol(n_ob, n_co, r_ob, r_co, floor_hit_ob=fh) == grade


def test_constants_match_contract():
    assert F.REF_MIN_LINES == 1200
    assert F.REF_MAX_RMS_MS == 150
    assert F.WAVSOL_VERSION == 2 and F.SHIFTS_VERSION == 2


def test_quality_record_from_fit_info():
    q = F.wavsol_quality_record(1500, 1600, 110.0, 105.0,
                                {"n_initial": 1700, "floor_hit": False},
                                {"n_initial": 1650, "floor_hit": True})
    assert q == {"nlines_ob": 1500, "nlines_co": 1600, "rms_ob": 110.0, "rms_co": 105.0,
                 "floor_hit_ob": False, "floor_hit_co": True, "n_initial_ob": 1700,
                 "n_initial_co": 1650, "grade": "degraded"}


# --------------------------------------------------------------------------
# reference selection
# --------------------------------------------------------------------------
class TestSelectReference:

    def test_min_metric_among_good(self):
        grades = ["bad", "good", "degraded", "good", "good"]
        difs = [0.0001, 0.3, 0.0002, 0.1, 0.2]           # the bad one has the best metric
        assert F.select_reference(grades, difs, None) == (3, "good")

    def test_degraded_when_nothing_is_good(self):
        grades = ["bad", "degraded", "degraded"]
        assert F.select_reference(grades, [0.0, 0.5, 0.4], None) == (2, "degraded")

    def test_least_rms_when_all_bad(self):
        grades = ["bad"] * 3
        assert F.select_reference(grades, [0.1, 0.2, 0.3], [6573.0, 22000.0, 810.0]) == (2, "bad")

    def test_without_metric_first_of_tier(self):
        """<= 6 ThArs: no metric, first (earliest) eligible frame, like refidx = 0."""
        assert F.select_reference(["good", "good"], None, None) == (0, "good")
        assert F.select_reference(["bad", "good", "good"], None, None) == (1, "good")

    def test_nan_metric_is_ignored(self):
        assert F.select_reference(["good", "good"], [np.nan, 0.5], None) == (1, "good")

    def test_ties_keep_the_first(self):
        assert F.select_reference(["good"] * 3, [0.2, 0.1, 0.1], None) == (1, "good")

    def test_empty_raises(self):
        with pytest.raises(ValueError):
            F.select_reference([], None, None)


# --------------------------------------------------------------------------
# legacy (pre-1.2) pickles
# --------------------------------------------------------------------------
def _fake_fibre(rng, n, rms_ms, suf=""):
    wav = rng.uniform(3600.0, 9200.0, n)
    # Post-culling residuals: a finished pre-1.2 cull leaves no 4-sigma outliers.
    res = wav * np.clip(rng.normal(0.0, rms_ms, n), -3.5 * rms_ms, 3.5 * rms_ms) / C   # Angstrom
    return {"G_wav" + suf: wav, "G_res" + suf: res, "II" + suf: list(range(n)),
            "G_pix" + suf: rng.uniform(50, 4000, n), "G_ord" + suf: rng.integers(8, 33, n).astype(float),
            "p1" + suf: np.zeros(33), "All_Wavelengths" + suf: np.r_[wav, wav[:10]],
            "All_Orders" + suf: np.r_[np.arange(8.0, 33.0), np.zeros(n + 10 - 25) + 20.0]}


def _fake_pdict(rng, n_ob, rms_ob, n_co=None, rms_co=None, version=1):
    p = {"mjd": 61300.0}
    p.update(_fake_fibre(rng, n_ob, rms_ob))
    p.update(_fake_fibre(rng, n_co or n_ob, rms_co or rms_ob, "_co"))
    p["rms_ms"] = 90.0            # the per-order value pre-1.2 pickles carry
    p["rms_ms_co"] = rms_co or rms_ob
    if version >= 2:
        p["wavsol_version"] = version
        p["quality"] = F.wavsol_quality_record(n_ob, n_co or n_ob, rms_ob, rms_co or rms_ob)
    return p


def test_legacy_rms_is_global_and_in_ms():
    rng = np.random.default_rng(1)
    q = F.wavsol_quality(_fake_pdict(rng, 1600, 104.0, 1700, 101.0))
    assert q["nlines_ob"] == 1600 and q["nlines_co"] == 1700
    assert q["rms_ob"] == pytest.approx(104.0, rel=0.06)   # not the stored 90 m/s
    assert q["rms_co"] == pytest.approx(101.0, rel=0.06)
    assert q["grade"] == "good"


def test_legacy_outliers_left_in_count_as_floor_hit():
    rng = np.random.default_rng(2)
    p = _fake_pdict(rng, 1600, 100.0)
    p["G_res"][:5] = p["G_wav"][:5] * 5000.0 / C          # misidentified lines never culled
    q = F.wavsol_quality(p)
    assert q["floor_hit_ob"] is True
    assert q["grade"] in ("degraded", "bad")


def test_legacy_residual_mismatch_is_reevaluated_from_p1():
    """A pre-1.2 fit that stopped culling on crossing minlines returned the
    residuals of the previous iteration; the kept lines are re-evaluated."""
    rng = np.random.default_rng(3)
    n, oo0, ntot, npix = 1100, 26, 25, 4096
    pix = rng.uniform(50, 4000, n)
    ords = rng.integers(8, 8 + ntot, n).astype(float)
    p1 = np.zeros(33)
    p1[0], p1[1], p1[6] = 2.3e5, 1.1e3, 4.0e2
    chebs = G.Calculate_chebs(pix, ords + oo0, Inverse=True, order0=oo0, ntotal=ntot, npix=npix, nx=5, nm=7)
    model = (1.0 / (ords + oo0)) * G.Joint_Polynomial_Cheby(p1, chebs, nx=5, nm=7)
    dv = rng.normal(0.0, 3000.0, n)                        # 3 km/s scatter of the kept lines
    wav = model / (1.0 + dv / C)
    fib = {"G_wav": wav, "G_pix": pix, "G_ord": ords, "p1": p1, "II": list(range(n)),
           "G_res": np.zeros(n + 37),                    # stale, longer than the kept lines
           "All_Wavelengths": np.zeros(n + 37), "All_Orders": np.r_[ords, np.zeros(37) + 8.0]}
    p = dict(fib)
    p.update({k + "_co": v for k, v in fib.items()})
    q = F.wavsol_quality(p)
    assert q["rms_ob"] == pytest.approx(np.std(dv), rel=1e-3)
    assert q["floor_hit_ob"] is True and q["grade"] == "bad"


def test_v2_pickle_uses_its_record():
    rng = np.random.default_rng(4)
    p = _fake_pdict(rng, 900, 120.0, version=2)
    p["G_res"] = p["G_res"] * 1000.0                       # ignored: the record wins
    q = F.wavsol_quality(p)
    assert q["nlines_ob"] == 900 and q["rms_ob"] == 120.0 and q["grade"] == "degraded"


def test_unreadable_pickle_grades_bad(tmp_path):
    bad = tmp_path / "x.wavsolpars.pkl"
    bad.write_bytes(b"not a pickle")
    assert F.load_wavsol_quality(str(bad))["grade"] == "bad"
    assert F.wavsol_quality({"II": [1, 2]})["grade"] == "bad"


# --------------------------------------------------------------------------
# science header cards
# --------------------------------------------------------------------------
GOOD_REF = {"grade": "good", "nlines_ob": 1650, "nlines_co": 1750, "rms_ob": 105.3, "rms_co": 101.2}
DEG_REF = {"grade": "degraded", "nlines_ob": 840, "nlines_co": 1170, "rms_ob": 123.0, "rms_co": 110.0}
BAD_REF = {"grade": "bad", "nlines_ob": 720, "nlines_co": 720, "rms_ob": 6573.0, "rms_co": 900.0}


def _cards(*a, **k):
    return {key: val for key, val, _ in F.wavsol_quality_cards(*a, **k)}


@pytest.mark.parametrize(
    "drift_ok, ref, err, wavsol, reason",
    [
        (True, GOOD_REF, 1.2, True, "ok"),
        (False, GOOD_REF, 22.34, False, "drift 22.3 m/s > 5"),
        (True, DEG_REF, 1.2, False, "ref degraded 840 lines"),
        (True, BAD_REF, 1.2, False, "ref bad rms 6573 m/s"),
        (False, BAD_REF, 22.3, False, "ref bad rms 6573 m/s; drift 22.3 m/s > 5"),
        (False, GOOD_REF, None, False, "no drift anchor"),          # ObjSky, no anchor
    ],
)
def test_card_values(drift_ok, ref, err, wavsol, reason):
    c = _cards(drift_ok, ref, err)
    assert c["HIERARCH GOOD QUALITY WAVSOL"] is wavsol
    assert c["HIERARCH GOOD QUALITY DRIFT"] is drift_ok
    assert c["HIERARCH GOOD QUALITY REFSOL"] is (ref["grade"] == "good")
    assert c["HIERARCH WAVSOL REF GRADE"] == ref["grade"]
    assert c["HIERARCH WAVSOL REF NLINES"] == min(ref["nlines_ob"], ref["nlines_co"])
    assert c["HIERARCH WAVSOL REF RMS"] == pytest.approx(max(ref["rms_ob"], ref["rms_co"]), abs=0.05)
    assert c["HIERARCH WAVSOL FLAG REASON"] == reason
    assert isinstance(c["HIERARCH CERES3 VERSION"], str)


def test_degraded_by_floor_hit_reason():
    ref = dict(GOOD_REF, grade="degraded")
    assert _cards(True, ref, 1.0)["HIERARCH WAVSOL FLAG REASON"] == "ref degraded cull floor hit"


def test_cards_fit_fits_headers():
    """update_header swallows exceptions, so a card that does not fit would
    silently vanish: every card must serialise, un-truncated, into 80 chars."""
    worst = {"grade": "bad", "nlines_ob": 99999, "nlines_co": 99999, "rms_ob": 1.2e7, "rms_co": 1.0}
    for drift_ok, ref, err in [(False, worst, 123456.78), (True, GOOD_REF, 1.0), (False, None, None)]:
        hdr = fits.Header()
        for key, val, com in F.wavsol_quality_cards(drift_ok, ref, err):
            with warnings.catch_warnings():
                warnings.simplefilter("error")
                card = fits.Card(key, val, com)
                assert len(card.image) == 80
            hdr[key] = (val, com)
            assert hdr[key] == val
        assert len(F.wavsol_flag_reason(drift_ok, ref, err)) <= 40


def test_cards_accept_missing_reference():
    c = _cards(True, None)
    assert c["HIERARCH GOOD QUALITY WAVSOL"] is False
    assert c["HIERARCH WAVSOL REF NLINES"] == -1


# --------------------------------------------------------------------------
# assess_calibration
# --------------------------------------------------------------------------
CONTRACT_KEYS = {"version", "source", "healthy", "trace_ok", "reference_ok", "reference_grade",
                 "reference_pkl", "reasons", "thar", "trace"}


def _trace_pkl(path: Path, rows=72, version=None):
    d = {"c_all": np.zeros((rows, 5)), "c_ob": np.zeros((rows // 2, 5)), "c_co": np.zeros((rows // 2, 5)),
         "nord_ob": rows // 2, "nord_co": rows // 2, "nord_all": rows, "GA_flat": 1.0, "RO_flat": 1.0}
    if version:
        d["trace_version"] = version
        d["label_info"] = {"n_raw": 74, "offset_px": 1.25, "matched": 72, "synthesized": [],
                           "max_residual_px": 0.05}
    with open(path / "trace.pkl", "wb") as f:
        pickle.dump(d, f)


def _raw(stem):
    return f"/data/spectra/raw_calibrations/2026-01-01/{stem}.fits"


def _legacy_dir(tmp_path, grades_rms, vals=None, version=1):
    """A calibration dir with one fake wavsolpars.pkl per (n_lines, rms) entry."""
    rng = np.random.default_rng(7)
    stems = [f"FEROS.2026-01-01T17-{i:02d}-00.000" for i in range(len(grades_rms))]
    for stem, (n, rms) in zip(stems, grades_rms):
        with open(tmp_path / f"{stem}.wavsolpars.pkl", "wb") as f:
            pickle.dump(_fake_pdict(rng, n, rms, version=version), f)
    if vals is not None:
        with open(tmp_path / "shifts.pkl", "wb") as f:
            pickle.dump({"vals": np.array(vals), "names": np.array([_raw(s) for s in stems])}, f)
    return stems


@pytest.fixture
def identity_trace(monkeypatch):
    """Decouple from the template matcher (another module section owns it)."""
    monkeypatch.setattr(F, "legacy_trace_ok", lambda c_all, npix: True, raising=False)


class TestAssessCalibration:

    def test_missing_dir_never_raises(self, tmp_path):
        r = F.assess_calibration(tmp_path / "nope_red")
        assert CONTRACT_KEYS <= set(r)
        assert r["healthy"] is False and r["reasons"]

    def test_corrupt_dir_never_raises(self, tmp_path):
        (tmp_path / "trace.pkl").write_bytes(b"\x00garbage")
        (tmp_path / "FEROS.x.wavsolpars.pkl").write_bytes(b"\x00garbage")
        (tmp_path / "shifts.pkl").write_bytes(b"\x00garbage")
        r = F.assess_calibration(tmp_path)
        assert r["healthy"] is False and r["trace_ok"] is False
        assert r["thar"]["n_bad"] == 1
        assert any("trace.pkl" in s for s in r["reasons"])

    def test_empty_dir(self, tmp_path):
        r = F.assess_calibration(tmp_path)
        assert r["healthy"] is False
        assert any("wavsolpars" in s for s in r["reasons"])

    def test_legacy_healthy_night(self, tmp_path, identity_trace):
        _trace_pkl(tmp_path)
        stems = _legacy_dir(tmp_path, [(1650, 104.0)] * 8, vals=[0.3, 0.2, 0.1, 0.4, 0.5, 0.6, 0.7, 0.8])
        r = F.assess_calibration(tmp_path)
        assert CONTRACT_KEYS <= set(r)
        assert r["source"] == "legacy" and r["healthy"] is True and r["reasons"] == []
        assert r["reference_grade"] == "good"
        assert Path(r["reference_pkl"]).name == f"{stems[2]}.wavsolpars.pkl"
        assert r["legacy_reference_grade"] == "good"
        assert r["thar"]["n_total"] == 8 and r["thar"]["n_good"] == 8

    def test_legacy_selection_picked_a_broken_reference(self, tmp_path, identity_trace):
        """The pre-1.2 argmin of the shifts metric landed on a broken ThAr (09-07)."""
        _trace_pkl(tmp_path)
        spec = [(1650, 104.0)] * 7 + [(840, 6573.0)]
        stems = _legacy_dir(tmp_path, spec, vals=[0.3, 0.2, 0.25, 0.4, 0.5, 0.6, 0.7, 0.01])
        r = F.assess_calibration(tmp_path)
        assert r["healthy"] is True and r["reference_grade"] == "good"
        assert Path(r["reference_pkl"]).name == f"{stems[1]}.wavsolpars.pkl"
        assert r["legacy_reference_grade"] == "bad"
        assert Path(r["legacy_reference_pkl"]).name == f"{stems[7]}.wavsolpars.pkl"
        assert any("pre-1.2" in s and "re-reduce" in s for s in r["reasons"])

    def test_no_healthy_reference(self, tmp_path, identity_trace):
        _trace_pkl(tmp_path)
        _legacy_dir(tmp_path, [(1000, 118.0), (900, 125.0), (720, 20000.0)])
        r = F.assess_calibration(tmp_path)
        assert r["healthy"] is False and r["reference_grade"] == "degraded"
        assert (r["thar"]["n_good"], r["thar"]["n_degraded"], r["thar"]["n_bad"]) == (0, 2, 1)
        assert any("no healthy reference ThAr" in s and "-ref_thar" in s for s in r["reasons"])

    def test_misregistered_legacy_trace(self, tmp_path, monkeypatch):
        monkeypatch.setattr(F, "legacy_trace_ok", lambda c_all, npix: False, raising=False)
        _trace_pkl(tmp_path)
        _legacy_dir(tmp_path, [(1650, 104.0)] * 3)
        r = F.assess_calibration(tmp_path)
        assert r["trace_ok"] is False and r["healthy"] is False
        assert r["trace"]["check"] == "legacy_trace_ok"

    def test_lost_traces(self, tmp_path, identity_trace):
        _trace_pkl(tmp_path, rows=64)
        _legacy_dir(tmp_path, [(1650, 104.0)] * 3)
        r = F.assess_calibration(tmp_path)
        assert r["trace_ok"] is False and r["trace"]["n_rows"] == 64
        assert any("64 traces" in s for s in r["reasons"])

    def test_row_count_fallback_without_template_matcher(self, tmp_path, monkeypatch):
        monkeypatch.delattr(F, "legacy_trace_ok", raising=False)
        _trace_pkl(tmp_path)
        _legacy_dir(tmp_path, [(1650, 104.0)] * 3)
        r = F.assess_calibration(tmp_path)
        assert r["trace_ok"] is True and r["trace"]["check"] == "row-count"

    def test_labelled_trace(self, tmp_path):
        _trace_pkl(tmp_path, version=2)
        _legacy_dir(tmp_path, [(1650, 104.0)] * 3, version=2)
        r = F.assess_calibration(tmp_path)
        assert r["trace_ok"] is True and r["trace"]["check"] == "labelled"
        assert r["trace"]["offset_px"] == 1.25
        assert "legacy_reference_pkl" not in r            # nothing pre-1.2 left

    def test_pipeline_report_round_trip(self, tmp_path):
        _trace_pkl(tmp_path, version=2)
        stems = _legacy_dir(tmp_path, [(1650, 104.0), (900, 120.0)], version=2)
        pkls = [str(tmp_path / f"{s}.wavsolpars.pkl") for s in stems]
        rep = F.calib_quality_report(str(tmp_path), thar_pkls=pkls, reference_pkl=pkls[0],
                                     warnings=["something actionable"])
        path = F.write_calib_quality(str(tmp_path), rep)
        on_disk = json.loads(Path(path).read_text())    # plain JSON, no NaN tokens
        assert on_disk["source"] == "pipeline" and on_disk["healthy"] is True
        assert on_disk["warnings"] == ["something actionable"]

        r = F.assess_calibration(tmp_path)
        assert CONTRACT_KEYS <= set(r)
        assert r["source"] == "pipeline" and r["healthy"] is True
        assert r["reference_grade"] == "good" and r["reference_pkl"] == pkls[0]
        assert r["thar"]["n_good"] == 1 and r["thar"]["n_degraded"] == 1

    def test_external_reference_is_graded(self, tmp_path):
        night, other = tmp_path / "2026-01-01_red", tmp_path / "2026-01-02_red"
        night.mkdir(), other.mkdir()
        _trace_pkl(night, version=2)
        _legacy_dir(night, [(1650, 104.0)], version=2)
        ext = _legacy_dir(other, [(840, 6573.0)])        # a broken pre-1.2 pickle
        rep = F.calib_quality_report(str(night), reference_pkl=str(other / f"{ext[0]}.wavsolpars.pkl"))
        assert rep["reference_grade"] == "bad" and rep["healthy"] is False
        assert any("-ref_thar" in s and "not healthy" in s for s in rep["reasons"])

    def test_pipeline_report_follows_a_moved_dir(self, tmp_path):
        _trace_pkl(tmp_path, version=2)
        stems = _legacy_dir(tmp_path, [(1650, 104.0)], version=2)
        rep = F.calib_quality_report(str(tmp_path))
        rep["reference_pkl"] = f"/data/spectra/calibrations/2026-01-01_red/{stems[0]}.wavsolpars.pkl"
        F.write_calib_quality(str(tmp_path), rep)
        r = F.assess_calibration(tmp_path)
        assert r["reference_pkl"] == str(tmp_path / f"{stems[0]}.wavsolpars.pkl")

    def test_unfinished_1_2_calibration_is_unhealthy(self, tmp_path, identity_trace):
        # traced by ceres3 >= 1.2 but no calib_quality.json: the -is_calib run died
        _trace_pkl(tmp_path, version=2)
        _legacy_dir(tmp_path, [(1650, 104.0)] * 2, version=2)
        r = F.assess_calibration(tmp_path)
        assert r["trace_ok"] is True and r["reference_ok"] is True
        assert r["healthy"] is False
        assert any("did not finish" in s for s in r["reasons"])

    def test_corrupt_json_falls_back_to_pickles(self, tmp_path, identity_trace):
        _trace_pkl(tmp_path)
        _legacy_dir(tmp_path, [(1650, 104.0)] * 2)
        (tmp_path / F.CALIB_QUALITY_FILE).write_text("{not json")
        r = F.assess_calibration(tmp_path)
        assert r["source"] == "legacy" and r["healthy"] is True
        assert any(F.CALIB_QUALITY_FILE in s for s in r["reasons"])


# --------------------------------------------------------------------------
# real production calibrations (skipped unless provided)
# --------------------------------------------------------------------------
def _real(night):
    root = os.environ.get("CERES3_FEROS_CALIB_ROOT")
    d = Path(root) / f"{night}_red" if root else None
    if d is None or not any(d.glob("*wavsolpars.pkl")):
        pytest.skip(f"real {night} calibration not available (set CERES3_FEROS_CALIB_ROOT)")
    return d


def test_real_2026_09_13_is_healthy():
    r = F.assess_calibration(_real("2026-09-13"))
    t = r["thar"]
    assert t["n_total"] > 0 and t["n_good"] == t["n_total"], [f for f in t["frames"] if f["grade"] != "good"]
    for f in t["frames"]:
        assert f["nlines_ob"] >= 1200 and max(f["rms_ob"], f["rms_co"]) < 110.0
    assert r["reference_grade"] == "good" and r["trace_ok"] is True and r["healthy"] is True


def test_real_2026_09_15_is_bad():
    r = F.assess_calibration(_real("2026-09-15"))
    t = r["thar"]
    assert t["n_total"] > 0 and t["n_bad"] == t["n_total"]
    assert r["reference_grade"] == "bad" and r["healthy"] is False
    assert r["trace_ok"] is False                        # 64 of 72 traces
    assert r["legacy_reference_grade"] == "bad"
