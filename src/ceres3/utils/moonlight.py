"""Scattered moonlight in a fibre, and whether it can bias a CCF RV (EXOAUTOMAT-301).

Moonlight is reflected sunlight. Through a stellar mask it gives a CCF dip at the
moon velocity, and where that dip overlaps the star's it pulls the single-Gaussian
centre. How much moonlight enters the fibre follows from the sky brightness the
Moon causes at the target (Krisciunas & Schaefer 1991, PASP 103, 1033) and from
the star's own count rate, so no catalogue magnitude is needed.

The prediction decides *whether* the two-Gaussian moon model is fitted; the fit
itself measures the moon component.
"""
from __future__ import annotations

import numpy as np
import astropy.units as u
from astropy.coordinates import AltAz, EarthLocation, SkyCoord, get_body
from astropy.time import Time
from scipy.optimize import least_squares


def ks_sky_brightness(phase_angle, separation, moon_zd, target_zd, k=0.12):
    """Sky surface brightness due to the Moon, in V mag/arcsec^2.

    phase_angle : lunar phase angle in degrees (0 = full, 180 = new).
    separation  : Moon-target angular distance in degrees.
    moon_zd, target_zd : zenith distances in degrees.
    k : V-band extinction coefficient (mag/airmass).

    Returns inf when the Moon is at or below the horizon: then it adds no light.
    """
    if not np.isfinite(moon_zd) or moon_zd >= 90.0:
        return np.inf
    target_zd = min(float(target_zd), 89.0)

    def airmass(zd):
        return (1.0 - 0.96 * np.sin(np.radians(zd)) ** 2) ** -0.5

    alpha = abs(float(phase_angle))
    illuminance = 10 ** (-0.4 * (3.84 + 0.026 * alpha + 4e-9 * alpha ** 4))
    rho = float(separation)
    scattering = 10 ** 5.36 * (1.06 + np.cos(np.radians(rho)) ** 2) + 10 ** (6.15 - rho / 40.0)
    b_nl = (scattering * illuminance * 10 ** (-0.4 * k * airmass(moon_zd))
            * (1.0 - 10 ** (-0.4 * k * airmass(target_zd))))          # nanoLamberts
    return (20.7233 - np.log(b_nl / 34.08)) / 0.92104


def moon_geometry(mjd, ra, dec, longitude, latitude, height):
    """Moon and target positions for an exposure.

    mjd is UTC (mid-exposure); ra/dec in degrees; site in degrees and metres.
    Returns moon_alt, target_alt, phase_angle and separation, all in degrees.
    """
    t = Time(mjd, format='mjd', scale='utc')
    site = EarthLocation.from_geodetic(longitude * u.deg, latitude * u.deg, height * u.m)
    frame = AltAz(obstime=t, location=site)
    moon = get_body('moon', t, site)
    sun = get_body('sun', t, site)
    target = SkyCoord(ra * u.deg, dec * u.deg)
    return {
        'moon_alt': float(moon.transform_to(frame).alt.deg),
        'target_alt': float(target.transform_to(frame).alt.deg),
        'phase_angle': 180.0 - float(sun.separation(moon).deg),
        'separation': float(moon.separation(target).deg),
    }


def moonlight_ratio(sky_mag, star_rate, zp_full, fibre_area):
    """Moonlight / starlight entering the fibre.

    sky_mag    : moonlit sky brightness, V mag/arcsec^2 (inf when the Moon is down).
    star_rate  : the star's measured count rate near 5500 A.
    zp_full    : V magnitude that gives one count per unit time with the whole
                 source inside the fibre.
    fibre_area : fibre aperture on the sky, arcsec^2.
    """
    if not np.isfinite(sky_mag):
        return 0.0
    if not (np.isfinite(star_rate) and star_rate > 0):
        return np.inf
    return fibre_area * 10 ** (-0.4 * (sky_mag - zp_full)) / star_rate


def _gauss_dip(v, depth, mu, sigma):
    return depth * np.exp(-0.5 * ((v - mu) / sigma) ** 2)


def predicted_bias(vels, depth, mu, sigma, moon_depth, moon_vel, moon_sigma, sigma_res=4.0):
    """Shift of the single-Gaussian CCF centre that a moon dip would cause.

    The star is the frame's own single-Gaussian fit (depth, mu, sigma); the moon
    dip (moon_depth, moon_vel, moon_sigma) is added to it and the single Gaussian
    refitted on the pipeline's window |v - mu| <= sigma_res * sigma, on the
    frame's velocity grid. Same units as the velocities.
    """
    vels = np.asarray(vels, dtype=float)
    if not (moon_depth > 0 and np.isfinite(moon_vel) and depth > 0 and sigma > 0):
        return 0.0
    w = np.abs(vels - mu) <= sigma_res * sigma
    if w.sum() < 4:
        return 0.0
    v = vels[w]
    clean = 1.0 - _gauss_dip(v, depth, mu, sigma)
    moonlit = clean - _gauss_dip(v, moon_depth, moon_vel, moon_sigma)

    def centre(y):
        r = least_squares(lambda p: 1.0 - _gauss_dip(v, *p) - y, [depth, mu, sigma],
                          bounds=([0.0, -np.inf, 1e-3], [2.0, np.inf, np.inf]))
        return r.x[1]

    return float(centre(moonlit) - centre(clean))
