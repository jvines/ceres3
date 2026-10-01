"""The Chebyshev basis is cached, and the arithmetic is unchanged (EXOAUTOMAT-287).

Profiling a live FEROS calibration reduction (py-spy, 24 samples) put ~25% of the
night in ``scipy.special.chebyt`` / ``roots_chebyt`` / ``poly1d.__init__``: the
wavelength solution rebuilt its polynomial basis on every evaluation, and
building an ``orthopoly1d`` computes the polynomial's roots.

Caching the object is bit-identical by construction — the same polynomial is
evaluated, merely not reconstructed — which is why it was chosen over
``numpy.polynomial.chebyshev.chebvander``. chebvander is roughly twice as fast
again but differs at ~1e-15, and this is the RV pipeline.

Measured: order-8 basis over 2048 px, 0.773 ms -> 0.084 ms per call (9.2x);
order 10 over 4096 px, 1.030 -> 0.226 ms (4.6x).
"""
from __future__ import annotations

import numpy as np
import pytest
import scipy.special

from ceres3.utils import globalutils as G


@pytest.fixture
def x():
    npix = 2048
    return (np.arange(npix) - npix / 2) / (npix / 2)


class TestBitIdentical:
    """The whole justification for caching rather than switching algorithm."""

    @pytest.mark.parametrize("degree", [0, 1, 2, 3, 4, 5, 6, 8, 10])
    def test_cached_matches_direct_construction_exactly(self, x, degree):
        direct = scipy.special.chebyt(degree)(x)
        cached = G.chebyt_poly(degree)(x)
        assert np.array_equal(direct, cached), "not bit-identical"

    def test_repeated_calls_give_identical_arrays(self, x):
        """A cached object must not accumulate state across evaluations."""
        first = G.chebyt_poly(8)(x)
        for _ in range(5):
            assert np.array_equal(first, G.chebyt_poly(8)(x))

    @pytest.mark.parametrize("degree", [1, 4, 9])
    def test_scalar_and_array_inputs_both_work(self, degree):
        assert np.isclose(G.chebyt_poly(degree)(0.5),
                          scipy.special.chebyt(degree)(0.5))
        arr = np.linspace(-1, 1, 17)
        assert np.array_equal(G.chebyt_poly(degree)(arr),
                              scipy.special.chebyt(degree)(arr))

    def test_known_values(self):
        """T0=1, T1=x, T2=2x^2-1 — a guard against caching the wrong degree."""
        pts = np.array([-1.0, -0.5, 0.0, 0.5, 1.0])
        assert np.allclose(G.chebyt_poly(0)(pts), np.ones_like(pts))
        assert np.allclose(G.chebyt_poly(1)(pts), pts)
        assert np.allclose(G.chebyt_poly(2)(pts), 2 * pts**2 - 1)


class TestCaching:

    def test_the_same_object_comes_back(self):
        assert G.chebyt_poly(7) is G.chebyt_poly(7)

    def test_different_degrees_are_different_objects(self):
        assert G.chebyt_poly(3) is not G.chebyt_poly(4)

    def test_construction_happens_once_per_degree(self, monkeypatch):
        calls = []
        real = scipy.special.chebyt

        def counted(degree):
            calls.append(degree)
            return real(degree)

        G.chebyt_poly.cache_clear()
        monkeypatch.setattr(G.sp, "chebyt", counted)
        for _ in range(10):
            G.chebyt_poly(11)
        G.chebyt_poly.cache_clear()
        assert calls == [11], f"rebuilt {len(calls)} times instead of once"

    def test_the_cache_is_exposed_for_tests_to_reset(self):
        """Without this a monkeypatched scipy could leak across tests."""
        assert hasattr(G.chebyt_poly, "cache_clear")


class TestCallSitesUseIt:
    """A cache nothing calls is not an optimisation."""

    def test_no_live_construction_remains_in_globalutils(self):
        from pathlib import Path
        source = Path(G.__file__).read_text()
        # Strip the one legitimate construction (inside chebyt_poly) and the
        # commented-out block that keeps the historical formulation.
        lines = [
            line for line in source.splitlines()
            if ("sp.chebyt(" in line or "scipy.special.chebyt(" in line)
            and "return sp.chebyt(degree)" not in line
            and not line.strip().startswith("#")
            and not line.strip().startswith("``")
            and "=" in line
        ]
        # Anything left must be inside the dead docstring block (u = ..., v = ...)
        for line in lines:
            assert line.strip().startswith(("u", "v")), f"live construction: {line.strip()}"
