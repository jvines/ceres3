"""Marsh optimal extraction gives exactly the numbers ceres3 1.3.1 gave (EXOAUTOMAT-304).

The extraction and the order-profile fit were rewritten for speed: band-sized
arrays instead of full-detector copies, settled columns skipped in the cosmic-ray
loop, and the profile fit's normal equations summed over overlapping pixels only.
None of that may change a single bit. The reference was produced by the 1.3.1
extension on this synthetic order: a curved trace, a flat, and a science frame
with 60 cosmic rays, extracted with no, a 10-sigma and a 50-sigma rejection.
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest

from ceres3.ext import Marsh

REF = Path(__file__).resolve().parent / "data" / "marsh_reference_1.3.1.npz"


def inputs():
    rng = np.random.default_rng(304)
    nr, nc = 80, 400
    x = np.arange(nc, dtype=float)
    center = 40.0 + 5.0 * np.sin(x / 80.0)
    rows = np.arange(nr, dtype=float)[:, None]
    prof = np.exp(-0.5 * ((rows - center[None, :]) / 1.8) ** 2)
    flat = 20000.0 * prof + 50.0 + rng.normal(0.0, 5.0, (nr, nc))
    sci = 3000.0 * prof * (1.0 + 0.2 * np.sin(x / 13.0)) + 30.0 + rng.normal(0.0, 8.0, (nr, nc))
    for _ in range(60):
        sci[rng.integers(0, nr), rng.integers(0, nc)] += rng.uniform(2000.0, 20000.0)
    return nr, nc, center, np.ascontiguousarray(flat), np.ascontiguousarray(sci)


@pytest.fixture(scope="module")
def run():
    nr, nc, center, flat, sci = inputs()
    P = np.asarray(Marsh.ObtainP(flat.flatten(), center, nr, nc, nc, 6.0, 3.0, 1.2, 10.0, 0.4, 4, 0, 5, nc - 5)).copy()
    P.resize(nr, nc)
    out = {"P": P}
    for ncos in (0.0, 10.0, 50.0):
        res, size = Marsh.ObtainSpectrum(sci.flatten(), center, P.flatten(), nr, nc, nc, 6.0, 3.0, 1.2, 0.4,
                                         ncos, 5, nc - 5)
        s = np.asarray(res).copy()
        s.resize(3, size)
        out[f"spec_cosmic{ncos:g}"] = s
    return out


@pytest.fixture(scope="module")
def ref():
    return np.load(REF)


@pytest.mark.parametrize("key", ["P", "spec_cosmic0", "spec_cosmic10", "spec_cosmic50"])
def test_bit_identical_to_1_3_1(run, ref, key):
    assert np.array_equal(run[key], ref[key])


def test_the_cosmic_ray_loop_is_exercised(run):
    # with rejection on, cosmic-hit columns change; with it off they keep the hits
    assert not np.array_equal(run["spec_cosmic10"][1], run["spec_cosmic0"][1])
