"""The global solution's inner loop, compiled (EXOAUTOMAT-287).

Once the Chebyshev basis construction and the line fit were dealt with, py-spy
put ~47% of the parent process in ``Joint_Polynomial_Cheby`` — a pure-Python
nested loop doing ~30 array multiply-adds over every line, on every residual
evaluation of the global wavelength solution.

The compiled kernel accumulates per element in exactly the order the interpreted
version accumulates per array, so every element sees the same additions in the
same sequence and the result is **bit-identical**. ``fastmath`` is off precisely
to keep that true.

Measured: 86 us interpreted, 35.0 us compiled including stacking the basis
(29.3 us kernel + 5.6 us stack). An element-major layout was tried to cut cache
misses: the kernel does improve to 26.9 us but transposing costs 15.9 us, so it
loses overall (42.9 vs 35.0) and term-major was kept.
"""
from __future__ import annotations

import numpy as np
import pytest

from ceres3.utils import globalutils as G


def _interpreted(p, chebs, nx, nm):
    """The original loop, kept here as the reference implementation."""
    xvec = chebs[:nx]
    mvec = chebs[nx:]
    ret_val = p[0]
    k = 1
    for i in range(nx):
        ret_val = ret_val + p[k] * xvec[i]
        k += 1
    for i in range(nm):
        ret_val = ret_val + p[k] * mvec[i]
        k += 1
    if nx >= nm:
        for i in range(nx):
            for j in range(min(nx - i, nm)):
                ret_val = ret_val + p[k] * xvec[i] * mvec[j]
                k += 1
    else:
        for j in range(nm):
            for i in range(min(nm - j - 1, nx)):
                ret_val = ret_val + p[k] * xvec[i] * mvec[j]
                k += 1
    return ret_val


def _case(nx, nm, npts=1500, seed=5):
    rng = np.random.default_rng(seed)
    chebs = [rng.uniform(-1, 1, npts) for _ in range(nx + nm)]
    return chebs, rng.uniform(-1, 1, 200)


class TestBitIdentical:

    @pytest.mark.parametrize("nx, nm", [(5, 6), (6, 5), (4, 4), (1, 1),
                                        (3, 7), (7, 3), (2, 6)])
    def test_matches_the_interpreted_loop_exactly(self, nx, nm):
        """Both branches of the nx >= nm split, and the symmetric case."""
        chebs, p = _case(nx, nm)
        assert np.array_equal(_interpreted(p, chebs, nx, nm),
                              G.Joint_Polynomial_Cheby(p, chebs, nx, nm))

    def test_a_single_point_works(self):
        chebs, p = _case(5, 6, npts=1)
        assert np.array_equal(_interpreted(p, chebs, 5, 6),
                              G.Joint_Polynomial_Cheby(p, chebs, 5, 6))

    def test_many_points_works(self):
        chebs, p = _case(5, 6, npts=20000)
        assert np.array_equal(_interpreted(p, chebs, 5, 6),
                              G.Joint_Polynomial_Cheby(p, chebs, 5, 6))

    @pytest.mark.skipif(not G.HAVE_NUMBA, reason="numba not installed")
    def test_the_interpreted_path_is_still_reachable(self, monkeypatch):
        """numba is an accelerator; the pure-Python path must still be correct."""
        chebs, p = _case(5, 6)
        compiled = G.Joint_Polynomial_Cheby(p, chebs, 5, 6)
        monkeypatch.setattr(G, "HAVE_NUMBA", False)
        interpreted = G.Joint_Polynomial_Cheby(p, chebs, 5, 6)
        assert np.array_equal(compiled, interpreted)


class TestSpeed:

    @pytest.mark.skipif(not G.HAVE_NUMBA, reason="numba not installed")
    def test_the_compiled_path_is_faster(self):
        import time

        chebs, p = _case(5, 6)
        G.Joint_Polynomial_Cheby(p, chebs, 5, 6)  # warm the JIT

        t0 = time.perf_counter()
        for _ in range(300):
            G.Joint_Polynomial_Cheby(p, chebs, 5, 6)
        compiled = (time.perf_counter() - t0) / 300

        t0 = time.perf_counter()
        for _ in range(300):
            _interpreted(p, chebs, 5, 6)
        interpreted = (time.perf_counter() - t0) / 300

        assert compiled < interpreted, (
            f"compiled {compiled*1e6:.1f} us vs interpreted {interpreted*1e6:.1f} us")
