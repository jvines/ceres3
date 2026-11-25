"""Early FEROS (2003-2004) lights the ThAr lamp through CALMIRR2 = LAMP1, not the
default LAMP3: FileClassify found no ThAr frame and the calibration died with
'no ThAr candidates to select a reference from'."""
from __future__ import annotations

import numpy as np
from astropy.io import fits

from ceres3.instruments import ferosutils_fp as F


def _wave(path, date, lamp):
    h = fits.PrimaryHDU(np.zeros((2, 2), dtype="f4"))
    h.header["HIERARCH ESO DPR TYPE"] = "WAVE"
    h.header["HIERARCH ESO INS CALMIRR2 ID"] = lamp
    h.header["DATE-OBS"] = date
    h.header["MJD-OBS"] = 53071.78
    h.header["EXPTIME"] = 10.0
    h.writeto(path)


def _classify(tmp_path, specs, **kw):
    for i, (date, lamp) in enumerate(specs):
        _wave(tmp_path / f"w{i}.fits", date, lamp)
    out = F.FileClassify(str(tmp_path) + "/", str(tmp_path / "night.log"), **kw)
    return out[2], out[3]


def test_early_night_on_lamp1_is_used_with_the_default_lamp(tmp_path):
    a, b = _classify(tmp_path, [("2004-03-07T18:48:31.258", "LAMP1")] * 3)
    assert len(a) == 3 and len(b) == 3


def test_early_night_requested_lamp_is_unchanged(tmp_path):
    a, _ = _classify(tmp_path, [("2004-03-07T18:48:31.258", "LAMP1"),
                                ("2004-03-07T18:50:31.258", "LAMP3"),
                                ("2004-03-07T18:52:31.258", "LAMP3")])
    assert len(a) == 2
    assert all("w0" not in p for p in a)


def test_modern_night_lamp3_is_unchanged(tmp_path):
    a, b = _classify(tmp_path, [("2020-03-07T18:48:31.258", "LAMP3")])
    assert a == [] and len(b) == 1
