"""
Drop-in replacement for the SWIG-wrapped C++ jplephem module.

Uses astropy for all ephemeris and barycentric correction calculations,
eliminating the need for SWIG, SOFA C library, and JPL binary ephemeris files.

Provides the same function signatures as the original jplephem module so that
pipeline scripts require no changes beyond the import path.
"""

import numpy as np
from astropy.coordinates import (
    SkyCoord, EarthLocation, AltAz,
    get_body_barycentric, get_body_barycentric_posvel,
    get_body, GCRS, ICRS
)
from astropy.time import Time
import astropy.units as u
import astropy.constants as const

# Module-level state (mirrors the C++ module's global state)
_observer_location = None
_ephemeris_dir = None
_ephemeris_name = None


def set_ephemeris_dir(path, ephemeris_name='DEc403'):
    """Set the ephemeris data directory. With astropy this is a no-op
    since astropy manages its own ephemeris data, but we store the
    values for compatibility."""
    global _ephemeris_dir, _ephemeris_name
    _ephemeris_dir = path
    _ephemeris_name = ephemeris_name


def set_observer_coordinates(x, y, z):
    """Set the observer's geocentric coordinates (in km).

    x: towards equator/Greenwich intersection
    y: 90 degrees east
    z: positive towards north pole

    These are geocentric Cartesian coordinates as computed by
    GLOBALutils.obspos(). Units are meters.
    """
    global _observer_location
    _observer_location = EarthLocation.from_geocentric(
        x * u.m, y * u.m, z * u.m
    )


def _make_time(mjd_int, mjd_frac):
    """Create an astropy Time object from integer MJD + fractional day."""
    return Time(mjd_int + mjd_frac, format='mjd', scale='utc')


def doppler_fraction(ra_hours, dec_deg, mjd_int, mjd_frac, n_steps=1, step_size=0.0):
    """Compute the Doppler fraction (barycentric velocity / c).

    Parameters
    ----------
    ra_hours : float
        Right ascension in hours (0-24).
    dec_deg : float
        Declination in degrees.
    mjd_int : int
        Integer part of MJD.
    mjd_frac : float
        Fractional part of MJD.
    n_steps : int
        Number of time steps (usually 1).
    step_size : float
        Step size in days between evaluations.

    Returns
    -------
    dict with key 'frac': list of Doppler fractions (v_bary / c).
    """
    fracs = []
    for i in range(n_steps):
        t = _make_time(mjd_int, mjd_frac + i * step_size)
        coord = SkyCoord(ra=ra_hours * 15.0 * u.deg, dec=dec_deg * u.deg,
                         frame='icrs')

        # Barycentric velocity correction
        vcorr = coord.radial_velocity_correction(
            obstime=t, location=_observer_location, kind='barycentric'
        )
        # Return as fraction of c (same as original jplephem)
        frac = (vcorr / const.c).decompose().value
        fracs.append(frac)

    return {'frac': fracs}


def pulse_delay(ra_hours, dec_deg, mjd_int, mjd_frac, n_steps=1, step_size=0.0):
    """Compute the light travel time correction (Roemer delay) in seconds.

    Parameters
    ----------
    Same as doppler_fraction.

    Returns
    -------
    dict with key 'delay': list of delays in seconds.
    """
    delays = []
    for i in range(n_steps):
        t = _make_time(mjd_int, mjd_frac + i * step_size)
        coord = SkyCoord(ra=ra_hours * 15.0 * u.deg, dec=dec_deg * u.deg,
                         frame='icrs')

        # Light travel time from observer to barycenter
        ltt = t.light_travel_time(coord, kind='barycentric',
                                  location=_observer_location)
        delays.append(ltt.to(u.s).value)

    return {'delay': delays}


def object_track(name, mjd_int, mjd_frac, n_steps=1, step_size=0.0):
    """Get the apparent RA/Dec of a solar system object.

    Parameters
    ----------
    name : str
        Object name (e.g. "Moon", "Sun").
    mjd_int : int
        Integer MJD.
    mjd_frac : float
        Fractional MJD.
    n_steps : int
        Number of steps.
    step_size : float
        Step in days.

    Returns
    -------
    dict with keys 'ra' (hours) and 'dec' (degrees).
    """
    ras = []
    decs = []
    for i in range(n_steps):
        t = _make_time(mjd_int, mjd_frac + i * step_size)
        body = get_body(name.lower(), t, location=_observer_location)
        body_icrs = body.transform_to(ICRS())
        ras.append(body_icrs.ra.hour)
        decs.append(body_icrs.dec.deg)

    return {'ra': ras, 'dec': decs}


def barycentric_object_track(name, mjd_int, mjd_frac, n_steps=1, step_size=0.0):
    """Get barycentric position and velocity of a solar system object.

    Parameters
    ----------
    name : str
        Object name (e.g. "Moon", "Sun").
    mjd_int : int
        Integer MJD.
    mjd_frac : float
        Fractional MJD.
    n_steps : int
        Number of steps.
    step_size : float
        Step in days.

    Returns
    -------
    dict with keys 'x', 'y', 'z' (AU), 'x_rate', 'y_rate', 'z_rate' (AU/day).
    """
    xs, ys, zs = [], [], []
    xrs, yrs, zrs = [], [], []
    for i in range(n_steps):
        t = _make_time(mjd_int, mjd_frac + i * step_size)
        pos, vel = get_body_barycentric_posvel(name.lower(), t)
        xs.append(pos.x.to(u.AU).value)
        ys.append(pos.y.to(u.AU).value)
        zs.append(pos.z.to(u.AU).value)
        xrs.append(vel.x.to(u.AU / u.day).value)
        yrs.append(vel.y.to(u.AU / u.day).value)
        zrs.append(vel.z.to(u.AU / u.day).value)

    return {
        'x': xs, 'y': ys, 'z': zs,
        'x_rate': xrs, 'y_rate': yrs, 'z_rate': zrs
    }


def object_doppler(name, mjd_int, mjd_frac, n_steps=1, step_size=0.0):
    """Compute the Doppler fraction for a solar system object.

    Parameters
    ----------
    name : str
        Object name (e.g. "Moon").
    mjd_int : int
        Integer MJD.
    mjd_frac : float
        Fractional MJD.
    n_steps : int
        Number of steps.
    step_size : float
        Step in days.

    Returns
    -------
    dict with key 'frac': list of Doppler fractions.
    """
    fracs = []
    for i in range(n_steps):
        t = _make_time(mjd_int, mjd_frac + i * step_size)

        # Get body position/velocity relative to observer
        body = get_body(name.lower(), t, location=_observer_location)
        body_icrs = body.transform_to(ICRS())

        # Compute the radial velocity correction towards the object
        vcorr = body_icrs.radial_velocity_correction(
            obstime=t, location=_observer_location, kind='barycentric'
        )
        frac = (vcorr / const.c).decompose().value
        fracs.append(frac)

    return {'frac': fracs}


def observer_position_velocity(mjd_int, mjd_frac, n_steps=1, step_size=0.0):
    """Get the observer's barycentric position and velocity.

    Returns
    -------
    dict with keys 'x', 'y', 'z' (AU), 'x_rate', 'y_rate', 'z_rate' (AU/day).
    """
    xs, ys, zs = [], [], []
    xrs, yrs, zrs = [], [], []
    for i in range(n_steps):
        t = _make_time(mjd_int, mjd_frac + i * step_size)
        pos, vel = get_body_barycentric_posvel('earth', t)
        # Add observer offset from geocenter
        if _observer_location is not None:
            obs_gcrs = _observer_location.get_gcrs_posvel(t)
            obs_pos = obs_gcrs[0].get_xyz()
            obs_vel = obs_gcrs[1].get_xyz()
            xs.append((pos.x + obs_pos[0]).to(u.AU).value)
            ys.append((pos.y + obs_pos[1]).to(u.AU).value)
            zs.append((pos.z + obs_pos[2]).to(u.AU).value)
            xrs.append((vel.x + obs_vel[0]).to(u.AU / u.day).value)
            yrs.append((vel.y + obs_vel[1]).to(u.AU / u.day).value)
            zrs.append((vel.z + obs_vel[2]).to(u.AU / u.day).value)
        else:
            xs.append(pos.x.to(u.AU).value)
            ys.append(pos.y.to(u.AU).value)
            zs.append(pos.z.to(u.AU).value)
            xrs.append(vel.x.to(u.AU / u.day).value)
            yrs.append(vel.y.to(u.AU / u.day).value)
            zrs.append(vel.z.to(u.AU / u.day).value)

    return {
        'x': xs, 'y': ys, 'z': zs,
        'x_rate': xrs, 'y_rate': yrs, 'z_rate': zrs
    }


def utc_to_tdb(mjd_int, mjd_frac):
    """Convert UTC MJD to TDB MJD."""
    t = _make_time(mjd_int, mjd_frac)
    tdb = t.tdb
    return {'tdb': tdb.mjd}


def utc_to_last(mjd_int, mjd_frac):
    """Convert UTC to Local Apparent Sidereal Time (hours)."""
    t = _make_time(mjd_int, mjd_frac)
    if _observer_location is not None:
        last = t.sidereal_time('apparent', longitude=_observer_location.lon)
    else:
        last = t.sidereal_time('apparent', longitude=0.0 * u.deg)
    return {'last': last.hour}
