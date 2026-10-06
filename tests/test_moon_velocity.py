"""The velocity of scattered moonlight matches SSEphem (EXOAUTOMAT-300).

The astropy stand-in for SSEphem had three faults from the port onwards:
object_track converted the Moon to ICRS, which re-centres it on the solar-system
barycentre; barycentric_object_track returned AU and AU/day where SSEphem returns
km and km/s; and object_doppler returned a star-style barycentric correction
instead of the Moon's radial velocity relative to the observer. For tau Ceti on
2020-12-13 the moon velocity came out +4.958 km/s against SSEphem's -0.052, so
every moon two-Gaussian fit and the moon-separation gate used the wrong velocity.

The reference values come from original CERES (rabrahm/ceres 72a95b2, SSEphem
with DE403) at 16 epochs from 2011 to 2026, for the FEROS site as ferospipe_fp
sets it.
"""
from __future__ import annotations

import json
from pathlib import Path

import ephem
import numpy as np
import pytest

from ceres3.utils import globalutils as G
from ceres3.utils import jplephem

REF = json.loads((Path(__file__).resolve().parent / "data" / "ssephem_moon_reference.json").read_text())
EPOCHS = REF["epochs"]
C_KMS = 2.99792458e5
TAU_CETI_2020_12_13 = 59197.02182915393


@pytest.fixture(autouse=True)
def feros_site():
    saved = jplephem._observer_location
    jplephem.set_observer_coordinates(*REF["obpos"])
    yield
    jplephem._observer_location = saved


def _call(fn, name, mjd):
    return fn(name, int(mjd), float(mjd % 1), 1, 0.0)


def _ssephem_moonvel(e):
    s = np.array([e["sun"][k] - e["moon"][k] for k in "xyz"])
    sv = np.array([e["sun"][k + "_rate"] - e["moon"][k + "_rate"] for k in "xyz"])
    return s.dot(sv) / np.linalg.norm(s) + e["moon_frac"] * C_KMS


def _ceres3_moonvel(mjd):
    gobs = ephem.Observer()
    gobs.lat, gobs.long = np.radians(-29.2543), np.radians(-70.7346)
    gobs.date = ephem.Date(mjd - 15019.5)          # pyephem counts Dublin Julian days
    Mcoo = _call(jplephem.object_track, "Moon", mjd)
    Mp = _call(jplephem.barycentric_object_track, "Moon", mjd)
    Sp = _call(jplephem.barycentric_object_track, "Sun", mjd)
    res = _call(jplephem.object_doppler, "Moon", mjd)
    return G.get_lunar_props(ephem, gobs, Mcoo, Mp, Sp, res, 26.017, -15.9375)[3]


def _ids(e):
    return f"mjd{e['mjd']:.2f}"


@pytest.mark.parametrize("e", EPOCHS, ids=_ids)
def test_moon_position_is_topocentric(e):
    track = _call(jplephem.object_track, "Moon", e["mjd"])
    ra1, dec1 = np.radians(track["ra"][0] * 15.0), np.radians(track["dec"][0])
    ra2, dec2 = np.radians(e["moon_ra_h"] * 15.0), np.radians(e["moon_dec"])
    cos_sep = np.sin(dec1) * np.sin(dec2) + np.cos(dec1) * np.cos(dec2) * np.cos(ra1 - ra2)
    assert np.degrees(np.arccos(min(1.0, cos_sep))) * 3600 < 60


@pytest.mark.parametrize("e", EPOCHS, ids=_ids)
@pytest.mark.parametrize("body", ["Moon", "Sun"])
def test_barycentric_state_is_in_km_and_km_per_s(e, body):
    state = _call(jplephem.barycentric_object_track, body, e["mjd"])
    ref = e[body.lower()]
    pos = np.array([state[k][0] - ref[k] for k in "xyz"])
    vel = np.array([state[k + "_rate"][0] - ref[k + "_rate"] for k in "xyz"])
    assert np.linalg.norm(pos) < 50.0          # km, of ~1.5e8
    assert np.linalg.norm(vel) < 1e-3          # km/s


@pytest.mark.parametrize("e", EPOCHS, ids=_ids)
def test_moon_doppler_is_relative_to_the_observer(e):
    frac = _call(jplephem.object_doppler, "Moon", e["mjd"])["frac"][0]
    assert abs(frac - e["moon_frac"]) * C_KMS < 1e-3   # 1 m/s


@pytest.mark.parametrize("e", EPOCHS, ids=_ids)
def test_moon_velocity_matches_ssephem(e):
    assert abs(_ceres3_moonvel(e["mjd"]) - _ssephem_moonvel(e)) < 1e-3   # 1 m/s


def test_the_tau_ceti_frame_that_exposed_it():
    # The port gave +4.958 km/s here; SSEphem gives -0.052.
    assert _ceres3_moonvel(TAU_CETI_2020_12_13) == pytest.approx(-0.052, abs=0.002)
