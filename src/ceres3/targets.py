"""Per-target metadata for a reduction: astrometry and CCF mask.

Replaces CERES's reffile.txt (name, sexagesimal J2000 position, proper motion added
linearly to RA and Dec without the cos(dec) term, mask, width). The targets file is
JSON::

    {"version": 1,
     "targets": {
        "<name as in the frame header>": {
            "ra": 26.0170, "dec": -15.9375,   # ICRS, degrees, at `epoch`
            "epoch": 2016.0,                  # Julian year of ra/dec
            "pmra": -1729.7, "pmdec": 855.5,  # mas/yr; pmra is mu_alpha* (includes cos dec)
            "parallax": 273.8,                # mas, optional
            "rv": -16.6,                      # km/s, optional
            "mask": "G2",                     # G2 | K5 | M2, optional
            "ccf_width": 4.0                  # km/s, optional
        }}}

A target may carry a mask without a position, or a position without a mask. The
position is propagated to each exposure with astropy's apply_space_motion before the
barycentric correction, so a high-proper-motion star's BERV no longer carries the
annual term a fixed J2000 position leaves (tau Ceti: K ~ 9 m/s).
"""
from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass, replace
from typing import Mapping, Optional

MASKS = ("G2", "K5", "M2")
SCHEMA_VERSION = 1
# A star with no parallax is put far enough away that perspective terms vanish.
_NO_PARALLAX_DISTANCE_PC = 1.0e6


class TargetsError(ValueError):
    """The targets file is malformed."""


@dataclass(frozen=True)
class Target:
    name: str
    ra: Optional[float] = None
    dec: Optional[float] = None
    epoch: float = 2000.0
    pmra: float = 0.0
    pmdec: float = 0.0
    parallax: Optional[float] = None
    rv: Optional[float] = None
    mask: Optional[str] = None
    ccf_width: Optional[float] = None

    @property
    def has_position(self) -> bool:
        return self.ra is not None and self.dec is not None


def normalise_name(name: str) -> str:
    """Case and whitespace/punctuation-insensitive key: 'tau Cet' == 'TAU_CET' == 'tauCet'."""
    return re.sub(r"[\s_\-]+", "", str(name)).casefold()


def _float(entry: Mapping, key: str, name: str, *, required: bool = False, default=None):
    value = entry.get(key, default)
    if value is None:
        if required:
            raise TargetsError(f"target {name!r}: {key} is required")
        return default
    try:
        value = float(value)
    except (TypeError, ValueError):
        raise TargetsError(f"target {name!r}: {key} must be a number, got {value!r}") from None
    if not math.isfinite(value):
        raise TargetsError(f"target {name!r}: {key} must be finite, got {value!r}")
    return value


def parse_target(name: str, entry: Mapping) -> Target:
    if not isinstance(entry, Mapping):
        raise TargetsError(f"target {name!r}: expected an object, got {type(entry).__name__}")
    has_ra, has_dec = entry.get("ra") is not None, entry.get("dec") is not None
    if has_ra != has_dec:
        raise TargetsError(f"target {name!r}: ra and dec must be given together")
    ra = _float(entry, "ra", name)
    dec = _float(entry, "dec", name)
    if ra is not None and not 0.0 <= ra < 360.0:
        raise TargetsError(f"target {name!r}: ra {ra} outside [0, 360)")
    if dec is not None and not -90.0 <= dec <= 90.0:
        raise TargetsError(f"target {name!r}: dec {dec} outside [-90, 90]")
    if ra is not None and entry.get("epoch") is None:
        raise TargetsError(f"target {name!r}: a position needs its epoch")
    mask = entry.get("mask")
    if mask is not None:
        mask = str(mask).strip().upper()
        if mask not in MASKS:
            raise TargetsError(f"target {name!r}: mask {entry.get('mask')!r} is not one of {MASKS}")
    parallax = _float(entry, "parallax", name)
    if parallax is not None and parallax <= 0:
        parallax = None
    width = _float(entry, "ccf_width", name)
    if width is not None and width <= 0:
        raise TargetsError(f"target {name!r}: ccf_width must be positive, got {width}")
    return Target(name=str(name), ra=ra, dec=dec, epoch=_float(entry, "epoch", name, default=2000.0),
                  pmra=_float(entry, "pmra", name, default=0.0), pmdec=_float(entry, "pmdec", name, default=0.0),
                  parallax=parallax, rv=_float(entry, "rv", name), mask=mask, ccf_width=width)


def parse_targets(doc: Mapping) -> dict[str, Target]:
    if not isinstance(doc, Mapping) or doc.get("version") != SCHEMA_VERSION:
        raise TargetsError(f"targets file must be an object with version {SCHEMA_VERSION}")
    raw = doc.get("targets")
    if not isinstance(raw, Mapping):
        raise TargetsError("targets file needs a 'targets' object")
    out: dict[str, Target] = {}
    for name, entry in raw.items():
        target = parse_target(name, entry)
        key = normalise_name(name)
        if key in out:
            # Two header spellings of one star ('tau Cet', 'tauCet') are fine as long
            # as they say the same thing.
            if replace(out[key], name="") != replace(target, name=""):
                raise TargetsError(f"targets {out[key].name!r} and {name!r} collide after normalisation")
            continue
        out[key] = target
    return out


def load_targets(path: Optional[str]) -> dict[str, Target]:
    """Targets keyed by normalised name; an absent path means no targets."""
    if not path:
        return {}
    with open(path) as fh:
        return parse_targets(json.load(fh))


def lookup(targets: Mapping[str, Target], name: str) -> Optional[Target]:
    return targets.get(normalise_name(name)) if targets else None


def position_at(target: Target, mjd_utc: float) -> tuple[float, float]:
    """ICRS (ra, dec) in degrees at the given UTC MJD, propagated with full space motion."""
    if not target.has_position:
        raise ValueError(f"target {target.name!r} has no position")
    import astropy.units as u
    from astropy.coordinates import SkyCoord
    from astropy.time import Time

    distance = (1000.0 / target.parallax) if target.parallax else _NO_PARALLAX_DISTANCE_PC
    coord = SkyCoord(ra=target.ra * u.deg, dec=target.dec * u.deg, frame="icrs",
                     pm_ra_cosdec=target.pmra * u.mas / u.yr, pm_dec=target.pmdec * u.mas / u.yr,
                     distance=distance * u.pc, radial_velocity=(target.rv or 0.0) * u.km / u.s,
                     obstime=Time(target.epoch, format="jyear", scale="tdb"))
    moved = coord.apply_space_motion(new_obstime=Time(mjd_utc, format="mjd", scale="utc"))
    return float(moved.ra.deg), float(moved.dec.deg)
