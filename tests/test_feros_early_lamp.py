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


def test_2003_frames_without_lamp_or_adc_keywords_classify(tmp_path):
    """2003 headers carry neither CALMIRR2 nor INS ADC1 RA/DEC."""
    h = fits.PrimaryHDU(np.zeros((2, 2), dtype="f4"))
    h.header["HIERARCH ESO DPR TYPE"] = "WAVE"
    h.header["DATE-OBS"] = "2003-07-09T11:46:46.000"
    h.header["MJD-OBS"] = 52829.49
    h.header["EXPTIME"] = 600.0
    h.writeto(tmp_path / "w.fits")
    s = fits.PrimaryHDU(np.zeros((2, 2), dtype="f4"))
    s.header["HIERARCH ESO DPR TYPE"] = "OBJECT,SKY"
    s.header["DATE-OBS"] = "2003-07-09T03:52:47.000"
    s.header["MJD-OBS"] = 52829.16
    s.header["EXPTIME"] = 600.0
    s.header["OBJECT"] = "hd181809"
    s.header["RA"] = 290.69817
    s.header["DEC"] = -20.63892
    s.writeto(tmp_path / "s.fits")
    out = F.FileClassify(str(tmp_path) + "/", str(tmp_path / "night.log"))
    assert len(out[2]) == 1 and len(out[3]) == 1 and len(out[5]) == 1
    assert "hd181809" in (tmp_path / "night.log").read_text()


def test_get_rg_without_read_mode_card():
    from ceres3.instruments import ferosutils as U
    assert U.get_RG(fits.Header()) == (7.2, 2.7)
    h = fits.Header()
    h["HIERARCH ESO DET READ MODE"] = "normal"
    assert U.get_RG(h) == (5.1, 3.2)


def test_overscan_trim_crops_2003_frames_to_4096_rows():
    from ceres3.instruments import ferosutils as U
    d = np.zeros((4102, 2148))
    d[1611, 300] = 1000.0
    out = U.OverscanTrim(d)
    assert out.shape == (4096, 2048)
    assert np.unravel_index(np.argmax(out), out.shape) == (1612, 250)
    assert U.OverscanTrim(np.ones((4096, 2148))).shape == (4096, 2048)
