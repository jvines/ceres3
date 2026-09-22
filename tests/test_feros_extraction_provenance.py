"""Science products cached by ferospipe_fp are only reused with the calibration
they were made with.

A science run reuses the extractions (*.spec.*.fits.S), scattered-light models
(BAC_*.fits), FP line positions and stellar parameters it finds in its output
directory. Re-reducing a night against another calibration -- an automatic
fallback night, or a night whose traces ceres3 >= 1.2 re-labelled -- used to keep
extractions made with the old traces while reporting the new calibration.
"""
from __future__ import annotations

import ast
import json
from pathlib import Path

import pytest

from ceres3.instruments import ferosutils_fp as F

PIPE = Path(__file__).resolve().parents[1] / "src" / "ceres3" / "instruments" / "ferospipe_fp.py"

CACHED = ["FEROS.x.spec.ob.fits.S", "FEROS.x.spec.co.fits.S", "FEROS.x.spec.simple.ob.fits.S",
          "FEROS.x.spec.simple.co.fits.S", "BAC_FEROS.x.fits", "FEROS.x.fplines.pkl"]
STELLAR = ["FEROS.x._stellar_pars.txt"]
KEEP = ["FEROS.x.tauCet_XC_G2.pkl", "night.log"]


def _calib(tmp_path, name="2026-09-13_red", trace=b"trace-a", flat=b"flat-a"):
    d = tmp_path / name
    d.mkdir()
    (d / "trace.pkl").write_bytes(trace)
    (d / "Flat.fits").write_bytes(flat)
    (d / "MasterBias.fits").write_bytes(b"bias")
    (d / "FEROS.r.wavsolpars.pkl").write_bytes(b"ref-a")
    return d


def _science(tmp_path):
    d = tmp_path / "sci_red"
    d.mkdir()
    for n in CACHED + STELLAR + KEEP:
        (d / n).write_bytes(b"x")
    return d


def _names(paths):
    return sorted(Path(p).name for p in paths)


def test_no_provenance_means_everything_cached_is_stale(tmp_path):
    cal, sci = _calib(tmp_path), _science(tmp_path)
    prov = F.extraction_provenance(cal, cal / "FEROS.r.wavsolpars.pkl")
    assert _names(F.stale_science_products(sci, prov)) == sorted(CACHED + STELLAR)


def test_same_calibration_keeps_the_cache(tmp_path):
    cal, sci = _calib(tmp_path), _science(tmp_path)
    prov = F.extraction_provenance(cal, cal / "FEROS.r.wavsolpars.pkl")
    F.write_extraction_provenance(sci, prov)
    assert F.stale_science_products(sci, prov) == []
    assert json.loads((sci / F.EXTRACTION_PROVENANCE_FILE).read_text())["trace_sha1"] == prov["trace_sha1"]


@pytest.mark.parametrize("what", ["trace", "flat", "dir"])
def test_another_calibration_invalidates_every_extraction(tmp_path, what):
    cal, sci = _calib(tmp_path), _science(tmp_path)
    F.write_extraction_provenance(sci, F.extraction_provenance(cal, cal / "FEROS.r.wavsolpars.pkl"))
    if what == "trace":
        (cal / "trace.pkl").write_bytes(b"trace-relabelled")
        other = cal
    elif what == "flat":
        (cal / "Flat.fits").write_bytes(b"flat-b")
        other = cal
    else:
        other = _calib(tmp_path, name="2026-09-12_red")
    prov = F.extraction_provenance(other, other / "FEROS.r.wavsolpars.pkl")
    assert _names(F.stale_science_products(sci, prov)) == sorted(CACHED + STELLAR)


def test_another_reference_only_invalidates_wavelength_products(tmp_path):
    cal, sci = _calib(tmp_path), _science(tmp_path)
    F.write_extraction_provenance(sci, F.extraction_provenance(cal, cal / "FEROS.r.wavsolpars.pkl"))
    ref = tmp_path / "healthy_night_ref.wavsolpars.pkl"
    ref.write_bytes(b"ref-b")
    assert _names(F.stale_science_products(sci, F.extraction_provenance(cal, ref))) == sorted(STELLAR)


def test_provenance_of_a_missing_directory_does_not_raise(tmp_path):
    prov = F.extraction_provenance(tmp_path / "nope")
    assert prov["trace_sha1"] is None and prov["reference_pkl"] is None


def test_pipeline_checks_provenance_before_the_science_loop():
    tree = ast.parse(PIPE.read_text(), filename=str(PIPE))
    loop = next(n for n in tree.body if isinstance(n, ast.For)
                and isinstance(n.iter, ast.Name) and n.iter.id == "comp_list")
    guard = [n for n in tree.body if isinstance(n, ast.If) and n.lineno < loop.lineno
             and "stale_science_products" in ast.unparse(n)]
    assert guard, "the provenance guard must run before the science loop"
    src = ast.unparse(guard[-1])
    assert "not is_calib" in src and "os.remove" in src and "write_extraction_provenance" in src
