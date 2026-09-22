"""Order detection noise estimate in ``get_them`` (ceres3 1.2, EXOAUTOMAT-268).

``get_them`` keeps a local maximum of the flat's central cut as an order when it
rises ``nsigmas`` times the inter-order background scatter above it. On FEROS
flats whose red-end comparison-fibre traces are weak (co/ob ~0.26 instead of
~0.49) those traces are only shoulders of their object-fibre neighbours, their
flux lands among the background samples, and the std of those samples comes
out at ~500 ADU instead of ~6. The inflated threshold then also drops the weak
blue orders: 46 of 2553 archive master flats traced to 60-68 raw orders instead
of 74, and odd counts (e.g. 65) crash the pipeline. ``robust_noise=True`` uses
1.4826*MAD, which those few samples cannot inflate; on all 2553 flats it left
the 74 traces of every good night unchanged and brought the 46 bad ones back to
70-73.

The fixture holds real 13-row median centre cuts (what ``get_them`` builds
internally) of three FEROS master flats: a good night and two of the bad ones.
They are tiled into a flat whose orders run straight along the rows, which
``get_them`` traces exactly as it would the real frame's central columns.
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest

from ceres3.utils import globalutils as G

DATA = Path(__file__).resolve().parent / "data" / "feros_flat_centrecuts.npz"
NCOLS = 41
MEDC = NCOLS // 2


@pytest.fixture(scope="module")
def cuts() -> dict[str, np.ndarray]:
    z = np.load(DATA)
    return {str(n): p for n, p in zip(z["names"], z["profs"])}


def trace(profile: np.ndarray, robust: bool) -> tuple[np.ndarray, int]:
    """Trace a flat built from one centre cut, with ferospipe_fp's arguments."""
    flat = np.tile(profile[:, None], (1, NCOLS))
    return G.get_them(flat, 6, 4, maxords=-1, mode=2, startfrom=40, endat=1900,
                      nsigmas=5.0, robust_noise=robust)


def centres(coefs: np.ndarray) -> np.ndarray:
    return np.array([np.polyval(c, MEDC) for c in coefs])


def test_default_and_robust_agree_on_a_good_flat(cuts):
    c_std, n_std = trace(cuts["2026-09-13"], robust=False)
    c_mad, n_mad = trace(cuts["2026-09-13"], robust=True)

    assert n_std == n_mad == 74
    np.testing.assert_array_equal(c_mad, c_std)


@pytest.mark.parametrize("night, n_default", [("2026-09-15", 66), ("2026-05-19", 65)])
def test_default_is_unchanged_on_bad_flats(cuts, night, n_default):
    # What production traced on these nights (before its c_all[2:]).
    assert trace(cuts[night], robust=False)[1] == n_default


@pytest.mark.parametrize("night", ["2026-09-15", "2026-05-19"])
def test_robust_noise_recovers_all_but_the_red_comparison_shoulders(cuts, night):
    good = centres(trace(cuts["2026-09-13"], robust=False)[0])
    std = centres(trace(cuts[night], robust=False)[0])
    mad = centres(trace(cuts[night], robust=True)[0])

    assert len(mad) == 72
    # Every order the default found is still there, at the same position (to
    # the few mpx the column-to-column tracing couples neighbouring orders by).
    assert all(np.min(np.abs(mad - x)) < 0.01 for x in std)
    # Matched to the good night's traces with one global offset, every trace is
    # a real order and only two good-night traces are missing: the weak
    # comparison fibres of the two reddest orders (2nd and 4th trace).
    offset = np.median([mad_x - good[np.argmin(np.abs(good - mad_x))] for mad_x in mad])
    nearest = np.array([np.argmin(np.abs(good - (x - offset))) for x in mad])
    assert np.max(np.abs(good[nearest] + offset - mad)) < 1.0
    assert len(set(nearest)) == 72
    assert sorted(set(range(74)) - set(nearest)) == [1, 3]
    # The blue end the default lost is back.
    assert std.max() < 1650 < mad.max()
