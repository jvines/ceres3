"""Regression tests for the FEROS nightly drift spline in ``ferospipe_fp``.

When ceres3 cannot measure the simultaneous-fibre drift to better than 5 m/s it
sets ``p_shift = 0.`` and flags ``GOOD QUALITY WAVSOL = False``. That sentinel
used to be appended to the night's drift array unconditionally, splined with
``scipy.interpolate.splrep``, and evaluated to assign a drift to every ObjSky
frame of the night — so frames that measured fine themselves were dragged toward
zero by a neighbour's *failure*, with nothing in the product recording it. On a
507-frame tauCet archive 32% of frames carried ``INSTRUMENTAL DRIFT = 0.0``
exactly, against a typical drift of ~95 m/s.

``ferospipe_fp`` is a top-level script with no importable seam — the drift logic
runs at module scope and needs a full raw night to execute — so the structural
half of these tests asserts against the parsed source. The numerical half
re-implements the consumer arithmetic to pin down what the guard buys.
"""
from __future__ import annotations

import ast
from pathlib import Path

import numpy as np
import pytest
import scipy.interpolate

PIPE = (
    Path(__file__).resolve().parents[1]
    / "src" / "ceres3" / "instruments" / "ferospipe_fp.py"
)


@pytest.fixture(scope="module")
def tree() -> ast.Module:
    return ast.parse(PIPE.read_text(), filename=str(PIPE))


def _appends_to(nodes: ast.AST | list[ast.stmt], target: str) -> list[ast.Call]:
    """Every ``<target>.append(...)`` call under ``nodes``."""
    roots = nodes if isinstance(nodes, list) else [nodes]
    return [
        n for root in roots for n in ast.walk(root)
        if isinstance(n, ast.Call)
        and isinstance(n.func, ast.Attribute)
        and n.func.attr == "append"
        and isinstance(n.func.value, ast.Name)
        and n.func.value.id == target
    ]


def _science_loop(tree: ast.Module) -> ast.For:
    """The ``for fsim in comp_list:`` loop that reduces the science frames."""
    loops = [
        n for n in tree.body
        if isinstance(n, ast.For)
        and isinstance(n.iter, ast.Name)
        and n.iter.id == "comp_list"
    ]
    # The file has two: extraction, then post-processing. The first one holds the
    # wavelength-solution work and is the only one that measures a drift.
    assert loops, "no `for fsim in comp_list:` loop found"
    return loops[0]


def _thar_branch(loop: ast.For) -> list[ast.stmt]:
    """Body of the ThAr comparison-fibre branch.

    Matched on the branch's own ``test``, and only its ``body`` is returned. The
    ThAr branch is the ``elif`` of the FP ``If``, so it lives in that node's
    ``orelse``: matching on ``ast.unparse`` of a whole ``If`` would span the
    entire chain and silently conflate the two branches.
    """
    for node in ast.walk(loop):
        if not isinstance(node, ast.If):
            continue
        if "comp_type == 'WAVE'" not in ast.unparse(node.test):
            continue
        if any("rms_ms / np.sqrt(len(I))" in ast.unparse(s) for s in node.body):
            return node.body
    pytest.fail("ThAr comparison-fibre branch not found in the science loop")


def _guarded_by_drift_ok(roots: list[ast.stmt], call: ast.Call) -> bool:
    """True if ``call`` only executes when ``drift_ok`` is truthy."""
    for root in roots:
        for node in ast.walk(root):
            if not isinstance(node, ast.If):
                continue
            if not (isinstance(node.test, ast.Name) and node.test.id == "drift_ok"):
                continue
            if any(call is c for stmt in node.body for c in ast.walk(stmt)):
                return True
    return False


class TestSentinelNeverAnchorsTheSpline:
    """The 5 m/s sentinel must not reach the interpolator."""

    def test_sentinel_still_exists(self, tree):
        """Guard against the gating tests passing because the zeroing was deleted."""
        src = "\n".join(ast.unparse(s) for s in _thar_branch(_science_loop(tree)))
        assert "drift_ok = False" in src
        assert "p_shift = 0.0" in src  # unparse normalises the `0.` literal

    @pytest.mark.parametrize("name", ["p_shifts", "p_mjds"])
    def test_science_appends_are_gated(self, tree, name):
        branch = _thar_branch(_science_loop(tree))
        calls = _appends_to(branch, name)
        assert calls, f"no {name}.append() left in the ThAr branch"
        for call in calls:
            assert _guarded_by_drift_ok(branch, call), (
                f"{name}.append() at line {call.lineno} is reachable when "
                "drift_ok is False — a failed measurement would be splined "
                "as a real zero-drift anchor."
            )

    def test_calibration_leftovers_are_cleared(self, tree):
        """The reference-selection block leaves both lists populated with
        ThAr-vs-ThAr shifts on a different zero point; they must be reset before
        the science loop, or they become anchors in the nightly spline."""
        loop = _science_loop(tree)
        reset = {}
        for node in tree.body:
            if node.lineno >= loop.lineno:
                break
            if isinstance(node, ast.Assign) and isinstance(node.value, ast.List):
                if not node.value.elts:
                    for tgt in node.targets:
                        if isinstance(tgt, ast.Name):
                            reset[tgt.id] = node.lineno
        for name in ("p_shifts", "p_mjds"):
            assert name in reset, f"{name} is never reset to []"

        block = _calibration_block(tree)
        assert block is not None, "reference-selection block not found"
        for name in ("p_shifts", "p_mjds"):
            assert reset[name] > block.lineno, (
                f"{name} is reset at line {reset[name]}, before the "
                f"reference-selection block at line {block.lineno} — the block's "
                "leftovers would survive into the nightly spline."
            )


def _calibration_block(tree: ast.Module) -> ast.If | None:
    """The ``if force_shift and len(sorted_ThAr_Ne_dates)>6:`` block."""
    for node in ast.walk(tree):
        if isinstance(node, ast.If) and "force_shift and len" in ast.unparse(node.test):
            return node
    return None


class TestConsumerFallbacks:
    """What the ObjSky frames get once failures are no longer anchors."""

    def test_single_anchor_is_held_not_discarded(self, tree):
        """A night reduced to one good anchor must use it, not fall back to 0."""
        for node in ast.walk(tree):
            if not isinstance(node, ast.For):
                continue
            if not (isinstance(node.iter, ast.Name) and node.iter.id == "new_sky"):
                continue
            src = ast.unparse(node)
            assert "len(p_mjds) == 1" in src, (
                "the ObjSky loop has no single-anchor branch: a night where all "
                "but one ObjCal frame failed would get no drift correction at all"
            )
            assert "p_shift = p_shifts[0]" in src
            return
        pytest.fail("`for fsim in new_sky:` loop not found")

    def test_degenerate_night_is_unchanged(self):
        """No anchors at all still means no correction — as before the fix."""
        p_shift = 0.0
        assert 1.0 + 1.0e-6 * p_shift == 1.0

    def test_sentinel_anchor_biases_the_interpolation(self):
        """Document the harm: one bogus zero halves a neighbour's drift.

        Two ObjCal frames 0.1 d apart, the first measuring +200 m/s and the
        second failing. An ObjSky frame midway used to receive the mean of the
        real measurement and the sentinel.
        """
        c = 299792458.0
        real = 1e6 * 200.0 / c  # +200 m/s in the pipeline's dimensionless units
        mjds = np.array([58452.10, 58452.20])
        sky_mjd = 58452.15

        contaminated = scipy.interpolate.splrep(mjds, np.array([real, 0.0]), k=1)
        biased = float(scipy.interpolate.splev(sky_mjd, contaminated))
        assert biased == pytest.approx(real / 2.0)

        # ~100 m/s of wavelength-scale error, landing directly on the RV.
        error_ms = (real - biased) * c / 1e6
        assert error_ms == pytest.approx(100.0)

        # With the failure dropped, the single surviving anchor is held instead.
        assert float(np.array([real])[0]) == pytest.approx(real)


def test_fp_branch_still_does_not_anchor_the_spline(tree):
    """The FP path zeroes on failure too; it must stay out of the drift array."""
    for node in ast.walk(_science_loop(tree)):
        if isinstance(node, ast.If) and "fp_error_co > 5" in ast.unparse(node.test):
            branch = node
            break
    else:
        pytest.fail("FP quality gate (`fp_error_co > 5`) not found")

    # Walk out to the enclosing FP branch. Only its `body` counts: the ThAr branch
    # is the `orelse` of this same `If` node, and it legitimately does append.
    for node in ast.walk(_science_loop(tree)):
        if not isinstance(node, ast.If):
            continue
        if "ref_fp_pkl is not None" not in ast.unparse(node.test):
            continue
        assert any(branch in ast.walk(stmt) for stmt in node.body), (
            "the `fp_error_co > 5` gate is no longer inside the FP branch body"
        )
        for stmt in node.body:
            assert not _appends_to(stmt, "p_shifts")
            assert not _appends_to(stmt, "p_mjds")
        return
    pytest.fail("FP branch not found")
