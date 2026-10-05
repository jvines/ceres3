"""FEROS orders are traced with CERES's get_them half-width of 5 (EXOAUTOMAT-296).

The port traced with 6. get_them scales its peak-finding smoothing (3x, 0.5x the
half-width) and its inter-order background windows (5/3 x) with it, and at 6 they
reach past the 12-18 px spacing of the object and comparison fibres: on flats whose
red comparison fibres are weak, the shoulders and the blue orders were lost (60-66
raw traces with the sample-std noise floor, 70-73 with the MAD floor added in 1.2).
Over the 44 archive flats that under-traced, half-width 5 with the MAD floor finds all
74 traces on 40 (6 on none, 5 with the sample-std floor on 36), and on the used orders
the positions agree with half-width 6 to 0.007 px (median).

The fixture holds real centre cuts of a good night (2026-09-13) and two of the bad
ones; see tests/test_get_them_noise.py for how they become a flat.
"""
from __future__ import annotations

import ast
from pathlib import Path

import numpy as np
import pytest

from ceres3.utils import globalutils as G

DATA = Path(__file__).resolve().parent / "data" / "feros_flat_centrecuts.npz"
PIPE = (Path(__file__).resolve().parents[1]
        / "src" / "ceres3" / "instruments" / "ferospipe_fp.py")
NCOLS = 41
MEDC = NCOLS // 2


@pytest.fixture(scope="module")
def cuts() -> dict[str, np.ndarray]:
    z = np.load(DATA)
    return {str(n): p for n, p in zip(z["names"], z["profs"])}


def trace(profile: np.ndarray, half_width: int, robust: bool) -> tuple[np.ndarray, int]:
    flat = np.tile(profile[:, None], (1, NCOLS))
    return G.get_them(flat, half_width, 4, maxords=-1, mode=2, startfrom=40, endat=1900,
                      nsigmas=5.0, robust_noise=robust)


def centres(coefs: np.ndarray) -> np.ndarray:
    return np.array([np.polyval(c, MEDC) for c in coefs])


def test_the_pipeline_traces_with_half_width_5():
    tree = ast.parse(PIPE.read_text(), filename=str(PIPE))
    consts = {t.id: n.value.value for n in tree.body if isinstance(n, ast.Assign)
              for t in n.targets if isinstance(t, ast.Name) and isinstance(n.value, ast.Constant)}
    assert consts["TRACE_HALF_WIDTH"] == 5
    assert "get_them(Flat, TRACE_HALF_WIDTH," in PIPE.read_text()


@pytest.mark.parametrize("night", ["2026-09-13", "2026-09-15", "2026-05-19"])
@pytest.mark.parametrize("robust", [False, True])
def test_half_width_5_finds_every_trace(cuts, night, robust):
    assert trace(cuts[night], 5, robust)[1] == 74


@pytest.mark.parametrize("night", ["2026-09-15", "2026-05-19"])
def test_half_width_6_loses_traces_on_the_weak_flats(cuts, night):
    # Why the port's value had to go: even with the MAD floor it misses traces.
    assert trace(cuts[night], 6, robust=True)[1] < 74


def test_positions_do_not_depend_on_the_half_width(cuts):
    c5 = centres(trace(cuts["2026-09-13"], 5, robust=True)[0])
    c6 = centres(trace(cuts["2026-09-13"], 6, robust=True)[0])
    assert len(c5) == len(c6) == 74
    # the two fibres of order -1 sit at the frame edge, where the windows differ
    np.testing.assert_allclose(c5[2:], c6[2:], atol=0.2)
