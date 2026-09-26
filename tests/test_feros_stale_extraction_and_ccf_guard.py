"""Two ways a FEROS night used to be lost, both found by the 2026-09-26
re-reduction of every affected night (EXOAUTOMAT-272).

1. A calibration directory whose trace.pkl had been relabelled by ceres3 >= 1.2
   while its ThAr extractions were still the pre-1.2 ones (31 or 32 orders
   instead of 36). The 1.2.0 guard correctly refused to calibrate with them, but
   it refused for good: four nights (2019-12-18, 2023-12-26, 2024-04-01,
   2024-09-03) could not be reduced at all. A cached extraction that disagrees
   with the trace is now redone instead.

2. XC_Final_Fit handed scipy.optimize.leastsq fewer residuals than free
   parameters on one faint frame of 2019-12-22 (2 CCF points, 3 parameters),
   and the TypeError took down that night's whole science run. Too few points
   is a failed fit for that frame, not a crash.
"""
from __future__ import annotations

import ast
from pathlib import Path

import numpy as np
import pytest
from astropy.io import fits

from ceres3.instruments import ferosutils_fp as F
from ceres3.utils import globalutils as G

PIPE = Path(__file__).resolve().parents[1] / "src" / "ceres3" / "instruments" / "ferospipe_fp.py"


def _spec(path: Path, nord: int) -> Path:
    fits.PrimaryHDU(np.zeros((nord, 3, 128), dtype=float)).writeto(path, overwrite=True)
    return path


class TestExtractionOrderMismatch:

    def test_agreeing_extraction_is_kept(self, tmp_path):
        p = _spec(tmp_path / "FEROS.thar.spec.ob.fits.S", 36)
        assert F.extraction_order_mismatch([str(p)], 36) is None

    @pytest.mark.parametrize("nord", [31, 32, 29])
    def test_disagreeing_extraction_is_reported(self, tmp_path, nord):
        p = _spec(tmp_path / "FEROS.thar.spec.ob.fits.S", nord)
        msg = F.extraction_order_mismatch([str(p)], 36)
        assert msg and f"holds {nord} orders" in msg and "another trace" in msg

    def test_any_disagreeing_file_is_enough(self, tmp_path):
        ob = _spec(tmp_path / "a.spec.ob.fits.S", 36)
        co = _spec(tmp_path / "a.spec.co.fits.S", 32)
        assert F.extraction_order_mismatch([str(ob), str(co)], 36)

    def test_missing_or_unreadable_files_are_not_a_mismatch(self, tmp_path):
        bad = tmp_path / "broken.spec.ob.fits.S"
        bad.write_bytes(b"not a FITS file")
        assert F.extraction_order_mismatch([str(tmp_path / "nope.fits"), str(bad), None], 36) is None

    def test_pipeline_rechecks_cached_thar_extractions(self):
        tree = ast.parse(PIPE.read_text(), filename=str(PIPE))
        src = ast.unparse(tree)
        assert "extraction_order_mismatch" in src, "the ThAr loop must re-check cached extractions"
        # and the mismatch must take part in the extract-or-load decision
        ifs = [n for n in ast.walk(tree) if isinstance(n, ast.If)
               and "stale_thar" in ast.unparse(n.test) and "force_thar_extract" in ast.unparse(n.test)]
        assert ifs, "a stale cached extraction must trigger re-extraction"


class TestCCFFitGuard:
    """X, Y with too few points inside the fitting window must not raise."""

    @staticmethod
    def _ccf(npts: int):
        # A narrow dip, sampled so that only ``npts`` points land within
        # sigma_res * sigma of the minimum.
        X = np.linspace(-60.0, 60.0, npts)
        Y = 1.0 - 0.4 * np.exp(-0.5 * (X / 3.0) ** 2)
        return X, Y

    @pytest.mark.parametrize("npts", [2, 3, 4])
    def test_few_points_returns_a_failed_fit_instead_of_raising(self, npts):
        X, Y = self._ccf(npts)
        p1, predicted, p1_gau, predicted_gau, L2 = G.XC_Final_Fit(
            X, Y, sigma_res=4, horder=8, moonv=0.0, moons=1.0, moon=False)
        assert np.all(np.isfinite(p1_gau))

    def test_a_well_sampled_ccf_still_fits_its_centre(self):
        X = np.linspace(-60.0, 60.0, 241)
        Y = 1.0 - 0.4 * np.exp(-0.5 * ((X - 5.0) / 4.0) ** 2)
        p1, predicted, p1_gau, predicted_gau, L2 = G.XC_Final_Fit(
            X, Y, sigma_res=4, horder=8, moonv=0.0, moons=1.0, moon=False)
        assert abs(p1_gau[1] - 5.0) < 1.0
