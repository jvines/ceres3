"""The observatory position handed to the ephemeris is where the observatory is.

JPLR0 took abs() of the geocentric latitude, so every southern site (La Silla,
Paranal, Las Campanas) was placed in the northern hemisphere: z = +3097 km for
FEROS instead of -3100 km. The barycentric velocity correction does not depend on
z (Earth's rotation velocity lies in the equatorial plane), but BJD moved by up to
~0.02 s and the topocentric Moon by ~1 deg.
"""
from __future__ import annotations

import astropy.units as u
import numpy as np
import pytest
from astropy.coordinates import EarthLocation

from ceres3.utils import globalutils as G

SITES = {
    "La Silla (FEROS)": (-70.7346, -29.2543, 2335.0),
    "Paranal": (-70.4045, -24.6272, 2635.0),
    "Calar Alto": (-2.5468, 37.2236, 2168.0),
    "equator": (10.0, 0.0, 0.0),
}


@pytest.mark.parametrize("name", SITES)
def test_matches_wgs84(name):
    lon, lat, h = SITES[name]
    r, z = G.JPLR0(lat, h)
    pos = np.array(G.obspos(lon, r, z))
    ref = np.array([q.to(u.m).value for q in EarthLocation.from_geodetic(lon * u.deg, lat * u.deg, h * u.m).geocentric])
    assert np.all(np.abs(pos - ref) < 5.0)          # metres


def test_a_southern_site_is_south_of_the_equator():
    r, z = G.JPLR0(-29.2543, 2335.0)
    assert z < 0
