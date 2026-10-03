"""obtain_P sums the per-order profiles as they arrive (EXOAUTOMAT-266).

Marsh's P for one order is a full-frame matrix. obtain_P collected every order's
matrix from pool.map and then copied them into one stack with np.array before
np.sum(axis=0): two full stacks alive at once. On a FEROS calibration night that
is 36 orders x 67 MB, twice, and it set the job's 5.05 GB peak regardless of how
many frames the night had (traced on kira, 2024-08-31).

It now consumes pool.imap in order and keeps a running sum. np.sum over axis 0
starts from 0.0 and adds the slices one after another, so the result is the same
bytes; that is what most of this file checks.
"""
from __future__ import annotations

import tracemalloc
from multiprocessing import Pool

import numpy as np
import pytest

from ceres3.utils import globalutils as G

# FEROS values from ferospipe_fp: ext_aperture, NSigma_Marsh, S_Marsh, N_Marsh,
# Marsh_alg, min/max_extract_col (scaled to the synthetic frame width).
APERTURE, NSIGMA, S, N, MARSH_ALG = 6, 10, 0.4, 4, 0
RON, GAIN = 5.1, 3.2
NY, NX = 300, 600


def _legacy_obtain_P(data, trace_coeffs, Aperture, RON, Gain, NSigma, S, N, Marsh_alg,
                     min_col, max_col, npools, pool=None):
    """globalutils.obtain_P as it was in ceres3 1.2.4, unchanged."""
    npars_paralel = []
    if isinstance(min_col, (int, float, np.integer, np.floating)):
        min_col = np.zeros(len(trace_coeffs)) + int(min_col)
    if isinstance(max_col, (int, float, np.integer, np.floating)):
        max_col = np.zeros(len(trace_coeffs)) + int(max_col)
    for i in range(len(trace_coeffs)):
        npars_paralel.append([trace_coeffs[i, :], Aperture, RON, Gain, NSigma, S, N, Marsh_alg,
                              int(min_col[i]), int(max_col[i])])
    if pool is not None:
        spec = np.array((pool.map(G.PCoeff2, npars_paralel)))
    else:
        spec = np.array((G._map_with_owned_pool(npools, (data,), G.PCoeff2, npars_paralel)))
    return np.sum(spec, axis=0)


def _flat(n_orders, seed=0):
    """A flat with n_orders gently tilted Gaussian traces, and their trace polynomials."""
    rng = np.random.default_rng(seed)
    x = np.arange(NX)
    rows = np.arange(NY)[:, None]
    data = rng.normal(0.0, RON, (NY, NX))
    coeffs = []
    for k in range(n_orders):
        c = np.array([2e-5, 0.01, 18.0 + 13.0 * k])
        centre = np.polyval(c, x)
        data += 8000.0 * (1 + 0.3 * np.sin(x / 70.0 + k)) * np.exp(-0.5 * ((rows - centre) / 1.6) ** 2)
        coeffs.append(c)
    return data, np.array(coeffs)


def _args(data, coeffs):
    return (data, coeffs, APERTURE, RON, GAIN, NSIGMA, S, N, MARSH_ALG, 50, NX - 50)


class TestRunningSum:

    def test_byte_identical_to_stacking_then_summing(self):
        rng = np.random.default_rng(1)
        arrays = [rng.random((64, 80)) * 10 ** rng.uniform(-8, 2) for _ in range(23)]
        arrays[3][5:9] = -0.0
        for a in arrays:            # np.sum turns a pixel that is -0.0 everywhere into +0.0
            a[0, :7] = -0.0
        expected = np.sum(np.array(arrays), axis=0)
        got = G._sum_in_order(iter(arrays))
        assert got.dtype == expected.dtype
        assert got.tobytes() == expected.tobytes()

    def test_the_first_array_is_not_modified(self):
        arrays = [np.ones((4, 4)), np.full((4, 4), 2.0)]
        G._sum_in_order(arrays)
        assert np.array_equal(arrays[0], np.ones((4, 4)))

    def test_no_orders_gives_what_np_sum_gave(self):
        assert G._sum_in_order(iter([])) == np.sum(np.array([]), axis=0)


class TestObtainPUnchanged:
    """Same profile as 1.2.4, through Marsh, for both pool paths."""

    def test_owned_pool(self):
        data, coeffs = _flat(5)
        new = G.obtain_P(*_args(data, coeffs), 2)
        old = _legacy_obtain_P(*_args(data, coeffs), 2)
        assert new.shape == data.shape
        assert np.count_nonzero(new) > 0
        assert new.tobytes() == old.tobytes()

    def test_shared_pool(self):
        data, coeffs = _flat(4, seed=2)
        with Pool(2, initializer=G._init_pool_worker, initargs=(data,)) as pool:
            new = G.obtain_P(*_args(data, coeffs), 2, pool=pool)
            old = _legacy_obtain_P(*_args(data, coeffs), 2, pool=pool)
        assert new.tobytes() == old.tobytes()

    def test_per_order_column_limits_still_apply(self):
        data, coeffs = _flat(3, seed=3)
        lo, hi = np.array([60, 80, 100]), np.array([500, 520, 540])
        args = (data, coeffs, APERTURE, RON, GAIN, NSIGMA, S, N, MARSH_ALG, lo, hi, 2)
        assert G.obtain_P(*args).tobytes() == _legacy_obtain_P(*args).tobytes()


class TestMemory:
    """Peak allocation in the parent, where the per-order matrices land."""

    ORDERS = 20

    @staticmethod
    def _peak(fn, *args):
        tracemalloc.start()
        try:
            fn(*args)
            return tracemalloc.get_traced_memory()[1]
        finally:
            tracemalloc.stop()

    def test_peak_is_a_few_frames_not_the_whole_stack(self):
        data, coeffs = _flat(self.ORDERS, seed=4)
        frame = data.nbytes
        peak = self._peak(G.obtain_P, *_args(data, coeffs), 2)
        assert peak < (self.ORDERS / 2) * frame, f"peak {peak / frame:.1f} frames for {self.ORDERS} orders"

    def test_peak_is_well_under_the_old_implementation(self):
        data, coeffs = _flat(self.ORDERS, seed=5)
        new = self._peak(G.obtain_P, *_args(data, coeffs), 2)
        old = self._peak(_legacy_obtain_P, *_args(data, coeffs), 2)
        assert new < 0.25 * old, f"new {new / 1e6:.1f} MB vs old {old / 1e6:.1f} MB"
