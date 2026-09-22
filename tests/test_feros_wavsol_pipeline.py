"""Structural guarantees of the FEROS wavelength-solution quality work in ``ferospipe_fp``.

Background (EXOAUTOMAT-268): when the ThAr lamp loses thorium, or a trace is
lost so that orders are calibrated with a neighbour's line list, the global
ThAr fits start below ``minlines`` and used to skip all outlier culling. The
resulting solutions have true global RMS of 0.4-28 km/s against ~105 m/s
healthy, yet the pipeline stored a per-order RMS as ``rms_ms``, accumulated the
order-by-order solutions across frames, picked the nightly reference ThAr with
no quality test at all, and flagged only the science-frame drift precision —
and only on OBJECT,WAVE frames. Wrong RVs were therefore ingested as good.

``ferospipe_fp`` is a top-level script with no importable seam, so, like
``test_feros_drift_spline.py``, these tests assert against the parsed source.
The grading itself is unit-tested in ``test_feros_wavsol_quality.py``.
"""
from __future__ import annotations

import ast
from pathlib import Path

import pytest

PIPE = (
    Path(__file__).resolve().parents[1]
    / "src" / "ceres3" / "instruments" / "ferospipe_fp.py"
)

GLOBAL_FITS = ("Fit_Global_Wav_Solution", "Global_Wav_Solution_vel_shift")


@pytest.fixture(scope="module")
def tree() -> ast.Module:
    return ast.parse(PIPE.read_text(), filename=str(PIPE))


def _calls(root: ast.AST, attr: str) -> list[ast.Call]:
    """Every ``<something>.<attr>(...)`` call under ``root``."""
    return [
        n for n in ast.walk(root)
        if isinstance(n, ast.Call)
        and isinstance(n.func, ast.Attribute)
        and n.func.attr == attr
    ]


def _assigned_names(node: ast.AST) -> set[str]:
    out = set()
    for tgt in getattr(node, "targets", []):
        for n in ast.walk(tgt):
            if isinstance(n, ast.Name):
                out.add(n.id)
    return out


def _thar_wavsol_loop(tree: ast.Module) -> ast.For:
    """The module-level loop that computes one wavsolpars.pkl per ThAr frame."""
    for node in tree.body:
        if isinstance(node, ast.For) and "wavsolpars.pkl" in ast.unparse(node) \
                and "Fit_Global_Wav_Solution" in ast.unparse(node):
            return node
    pytest.fail("per-ThAr wavelength-solution loop not found")


def _science_loop(tree: ast.Module) -> ast.For:
    loops = [
        n for n in tree.body
        if isinstance(n, ast.For)
        and isinstance(n.iter, ast.Name)
        and n.iter.id == "comp_list"
    ]
    assert loops, "no `for fsim in comp_list:` loop found"
    return loops[0]


def _fp_branch(tree: ast.Module) -> list[ast.stmt]:
    for node in ast.walk(_science_loop(tree)):
        if isinstance(node, ast.If) and "ref_fp_pkl is not None" in ast.unparse(node.test):
            return node.body
    pytest.fail("FP branch not found")


def _wave_branch(tree: ast.Module) -> list[ast.stmt]:
    for node in ast.walk(_science_loop(tree)):
        if isinstance(node, ast.If) and "comp_type == 'WAVE'" in ast.unparse(node.test) \
                and any("rms_ms / np.sqrt(len(I))" in ast.unparse(s) for s in node.body):
            return node.body
    pytest.fail("ThAr comparison-fibre (WAVE) branch not found")


def _sky_loop(tree: ast.Module) -> ast.For:
    for node in tree.body:
        if isinstance(node, ast.For) and isinstance(node.iter, ast.Name) and node.iter.id == "new_sky":
            return node
    pytest.fail("`for fsim in new_sky:` loop not found")


def _module_constant(tree: ast.Module, name: str):
    for node in tree.body:
        if isinstance(node, ast.Assign) and name in _assigned_names(node):
            return ast.literal_eval(node.value)
    pytest.fail(f"module constant {name} not found")


class TestGlobalFitsOptInToTheCullFloor:
    """Every global fit culls whenever outliers exist, bounded by a floor."""

    def test_feros_floor_values(self, tree):
        assert _module_constant(tree, "WAVSOL_CULL_FLOOR") == 300
        assert _module_constant(tree, "WAVSOL_MAX_CULL_FRAC") == 0.5

    @pytest.mark.parametrize("func", GLOBAL_FITS)
    def test_every_call_passes_floor_and_info(self, tree, func):
        calls = _calls(tree, func)
        assert calls, f"no {func} call found"
        for call in calls:
            kw = {k.arg: ast.unparse(k.value) for k in call.keywords}
            where = f"{func} call at line {call.lineno}"
            assert kw.get("cull_floor") == "WAVSOL_CULL_FLOOR", f"{where} does not opt in to the floor"
            assert kw.get("max_cull_frac") == "WAVSOL_MAX_CULL_FRAC", f"{where} has no max_cull_frac"
            assert "info" in kw and kw["info"] != "None", f"{where} does not collect fit info"

    def test_calibration_reference_and_science_are_all_covered(self, tree):
        """Guard against the kwargs test passing because call sites vanished."""
        loop = _thar_wavsol_loop(tree)
        assert len(_calls(loop, "Fit_Global_Wav_Solution")) == 2          # object + comparison
        wave = ast.Module(body=_wave_branch(tree), type_ignores=[])
        assert len(_calls(wave, "Fit_Global_Wav_Solution")) == 1
        assert len(_calls(wave, "Global_Wav_Solution_vel_shift")) == 1
        assert len(_calls(tree, "Global_Wav_Solution_vel_shift")) >= 3    # + reference selection


class TestPerFrameThArSolutions:

    def test_order_solutions_reset_per_frame(self, tree):
        """c_p2w / c_p2w_c used to be initialised once, outside the loop, so the
        pickle of the k-th ThAr carried the order solutions of all k frames."""
        loop = _thar_wavsol_loop(tree)
        for name in ("c_p2w", "c_p2w_c"):
            resets = [
                n for n in ast.walk(loop)
                if isinstance(n, ast.Assign) and name in _assigned_names(n)
                and "[]" in ast.unparse(n.value)
            ]
            assert resets, f"{name} is not reset inside the per-ThAr loop"
            appends = [c for c in _calls(loop, "append")
                       if isinstance(c.func.value, ast.Name) and c.func.value.id == name]
            assert appends, f"no {name}.append() in the per-ThAr loop"
            assert min(r.lineno for r in resets) < min(a.lineno for a in appends)
        for node in tree.body:
            if node.lineno >= loop.lineno:
                break
            assert not ({"c_p2w", "c_p2w_c"} & _assigned_names(node)), (
                "c_p2w/c_p2w_c are still initialised at module level before the loop"
            )

    @pytest.mark.parametrize("name", ["rms_ms", "rms_ms_co"])
    def test_stored_rms_is_the_global_fit(self, tree, name):
        """Only the global fit may bind rms_ms / rms_ms_co in the loop: the
        per-order Initial_Wav_Calibration used to overwrite 'rms_ms'."""
        loop = _thar_wavsol_loop(tree)
        binders = [n for n in ast.walk(loop) if isinstance(n, ast.Assign) and name in _assigned_names(n)]
        assert binders, f"{name} is never bound in the per-ThAr loop"
        for node in binders:
            assert _calls(node.value, "Fit_Global_Wav_Solution"), (
                f"line {node.lineno} binds {name} from something other than the global fit"
            )

    def test_pickle_records_version_and_quality(self, tree):
        loop = _thar_wavsol_loop(tree)
        dicts = [n.value for n in ast.walk(loop)
                 if isinstance(n, ast.Assign) and "pdict" in _assigned_names(n) and isinstance(n.value, ast.Dict)]
        assert len(dicts) == 1, "expected exactly one wavsolpars dict literal"
        entries = {ast.literal_eval(k): ast.unparse(v) for k, v in zip(dicts[0].keys, dicts[0].values)}
        assert entries["rms_ms"] == "rms_ms"
        assert entries["rms_ms_co"] == "rms_ms_co"
        assert entries["wavsol_version"] == "ferosutils_fp.WAVSOL_VERSION"
        assert "quality" in entries
        assert entries["c_p2w"] == "c_p2w" and entries["c_p2w_c"] == "c_p2w_c"

    def test_old_pickles_are_recomputed(self, tree):
        loop = _thar_wavsol_loop(tree)
        gates = [n for n in ast.walk(loop) if isinstance(n, ast.Compare)
                 and "wavsol_version" in ast.unparse(n.left)
                 and any("WAVSOL_VERSION" in ast.unparse(c) for c in n.comparators)]
        assert gates, "cached wavsolpars.pkl are not version-gated"

    def test_order_count_mismatch_is_fatal_not_clamped(self, tree):
        loop = _thar_wavsol_loop(tree)
        for node in ast.walk(loop):
            if isinstance(node, (ast.Assign, ast.AugAssign)):
                targets = _assigned_names(node) if isinstance(node, ast.Assign) else {getattr(node.target, "id", "")}
                assert "n_useful" not in targets, f"line {node.lineno} still clamps n_useful"
        raises = [n for n in ast.walk(loop) if isinstance(n, ast.Raise)]
        assert any("FerosTraceError" in ast.unparse(r.exc) for r in raises)
        assert "clamped" not in ast.unparse(loop)


class TestReferenceSelection:

    def test_selection_is_quality_gated(self, tree):
        calls = _calls(tree, "select_reference")
        assert len(calls) == 1, "reference must be chosen by ferosutils_fp.select_reference"
        src = ast.unparse(tree)
        assert "np.argmin(dct_shfts['vals'])" not in src, "ungated argmin selection is back"
        assert "refidx = difs" not in src

    def test_metric_only_spans_the_eligible_tier(self, tree):
        """Candidates and targets of the shift metric are both the best tier:
        worse frames never reach the O(N^2) one-line-at-a-time shift fits."""
        block = next(n for n in ast.walk(tree) if isinstance(n, ast.If)
                     and "force_shift and len" in ast.unparse(n.test) and _calls(n, "Global_Wav_Solution_vel_shift"))
        loops = [n for n in ast.walk(block) if isinstance(n, ast.While)]
        outer = min(loops, key=lambda n: n.lineno)
        inner = [n for n in loops if n is not outer]
        assert "thar_grades[j] != ref_tier" in "\n".join(ast.unparse(s) for s in outer.body[:4])
        assert inner and "thar_grades[i] != ref_tier" in ast.unparse(inner[0].body[0])

    def test_reference_is_graded_for_every_path(self, tree):
        """Including -ref_thar: the grade is taken after the ref_thar if/else."""
        ref_if = next(n for n in tree.body if isinstance(n, ast.If) and "ref_thar != None" in ast.unparse(n.test))
        graded = [n for n in tree.body if isinstance(n, ast.Assign) and "ref_quality" in _assigned_names(n)]
        assert graded, "ref_quality is not assigned at module level"
        assert _calls(graded[0].value, "wavsol_quality")
        assert graded[0].lineno > ref_if.end_lineno

    def test_shifts_cache_is_versioned_and_graded(self, tree):
        src = ast.unparse(tree)
        assert "dct_shfts['version'] = ferosutils_fp.SHIFTS_VERSION" in src
        assert "dct_shfts['grades'] = np.array(thar_grades)" in src
        assert "ferosutils_fp.SHIFTS_VERSION" in ast.unparse(
            next(n for n in ast.walk(tree) if isinstance(n, ast.Compare)
                 and "dct_shfts.get('version'" in ast.unparse(n))
        )


class TestScienceHeaderCards:
    """GOOD QUALITY WAVSOL (drift AND reference) is written for WAVE, FP and SKY."""

    @staticmethod
    def _writes_cards(roots) -> bool:
        roots = roots if isinstance(roots, list) else [roots]
        for root in roots:
            for node in ast.walk(root):
                if isinstance(node, ast.For) and _calls(node.iter, "wavsol_quality_cards") \
                        and any(_calls(s, "update_header") for s in node.body):
                    return True
        return False

    def test_wave_branch(self, tree):
        assert self._writes_cards(_wave_branch(tree))

    def test_fp_branch(self, tree):
        assert self._writes_cards(_fp_branch(tree))

    def test_sky_frames(self, tree):
        assert self._writes_cards(_sky_loop(tree))

    def test_sky_drift_flag_is_the_anchor_count(self, tree):
        src = ast.unparse(_sky_loop(tree))
        assert "drift_ok = len(p_mjds) >= 1" in src

    def test_cards_only_come_from_the_helper(self, tree):
        """One definition of the flag: no stray literal WAVSOL card writes."""
        literals = [n for n in ast.walk(tree) if isinstance(n, ast.Constant)
                    and n.value == "HIERARCH GOOD QUALITY WAVSOL"]
        assert not literals

    @pytest.mark.parametrize("branch", ["wave", "fp"])
    def test_drift_zeroing_tied_to_drift_only(self, tree, branch):
        body = _wave_branch(tree) if branch == "wave" else _fp_branch(tree)
        for node in ast.walk(ast.Module(body=body, type_ignores=[])):
            if isinstance(node, ast.If) and "p_shift = 0.0" in "\n".join(ast.unparse(s) for s in node.body):
                test = ast.unparse(node.test)
                assert ("precision > 5" in test or "fp_error_co > 5" in test) and "ref" not in test
                break
        else:
            pytest.fail(f"no drift zeroing in the {branch} branch")


def test_calibration_runs_write_calib_quality(tree):
    blocks = [n for n in tree.body if isinstance(n, ast.If) and ast.unparse(n.test) == "is_calib"
              and _calls(n, "write_calib_quality")]
    assert blocks, "-is_calib runs do not write calib_quality.json"
    assert _calls(blocks[-1], "calib_quality_report")
    assert blocks[-1].lineno > _sky_loop(tree).lineno, "calib_quality.json must be written at the end"
