"""The fine CCF is never computed wider than the rough search (EXOAUTOMAT-301).

The second CCF pass sizes its velocity grid as max(20, 6 disp), with disp taken from
the first pass's single-Gaussian width. On 2021-10-16 that fit diverged for
TIC399868187 (a width of ~1e12 km/s), the grid asked for 3.4 PiB, and the
MemoryError ended the night's post-processing: every frame after it lost its RV.
ceres3 1.3.1 does the same on that night.
"""
from __future__ import annotations

import ast
from pathlib import Path

PIPE = (Path(__file__).resolve().parents[1]
        / "src" / "ceres3" / "instruments" / "ferospipe_fp.py")


def src() -> str:
    return ast.unparse(ast.parse(PIPE.read_text(), filename=str(PIPE)))


def test_the_width_from_the_first_pass_is_capped_at_the_rough_search():
    s = src()
    block = s[s.index("if not known_sigma:"):]
    block = block[:block.index("known_sigma = True")]
    assert "disp = np.floor(p1gau[2]) if np.isfinite(p1gau[2]) else 3.0" in block
    assert "disp = min(disp, velw / 6.0)" in block
    # the cap comes before the mask is widened with it
    assert block.index("disp = min(disp, velw / 6.0)") < block.index("mask_hw_wide =")


def test_the_rough_search_and_the_fine_grid_agree():
    s = src()
    assert "velw = 300" in s
    assert "vel_width = np.maximum(20.0, 6 * disp)" in s
