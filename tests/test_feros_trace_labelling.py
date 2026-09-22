"""Regression tests for FEROS order-trace labelling (``ferosutils_fp.label_traces``).

``ferospipe_fp`` used to drop the first two traces ``GLOBALutils.get_them`` found
(``c_all[2:]``, meant to be the order -1 pair) and take the rest as alternating
object/comparison traces from order 0 upwards. On a master flat that misses a
trace -- the weak red-end comparison traces co-1/co0, or blue traces lost to a
noise estimate inflated by them -- every order was then wavelength-calibrated
with its neighbour's line list (5000-8800 km/s errors, no CCF dip), and an odd
trace count crashed the ob/co split. 46 of 2553 archive nights were affected.

``label_traces`` identifies each detected trace against a packaged template of
the 74 physical traces with one global offset. The fixtures are real
``get_them`` outputs (``raw_std_*`` = production noise estimate, ``raw_mad_*`` =
``robust_noise=True``) and production ``trace.pkl`` ``c_all`` arrays
(``legacy_*``), all from ``tests/data/feros_trace_fixtures.npz``:

* 2026-09-16: healthy night whose weak co0 trace was fitted 4.3 px off.
* 2026-09-15: co-1 and co0 missing; with the production noise estimate the blue
  orders 33-35 are lost too. Legacy c_all is shifted by one order.
* 2026-05-14: co0 missing, co-1 detected 4.6 px off: odd raw count before
  ``[2:]``, so the legacy c_all swaps the fibres from order 1 on.
* 2026-07-02: odd raw count that crashed the legacy split (IndexError).
* 2024-04-29: 66-row legacy c_all, fibres swapped.

The physical identity of the legacy rows was established independently by the
EXOAUTOMAT-268 investigation (trace alignment to 2026-09-13 and ThAr
cross-correlation of the extracted orders).
"""
from __future__ import annotations

import ast
from pathlib import Path

import numpy as np
import pytest

from ceres3.instruments import ferosutils_fp as F

ROOT = Path(__file__).resolve().parents[1]
PIPE = ROOT / "src" / "ceres3" / "instruments" / "ferospipe_fp.py"
NPIX = 4096
X_CHECK = (512, 2048, 3584)


@pytest.fixture(scope="module")
def fx() -> dict[str, np.ndarray]:
    with np.load(ROOT / "tests" / "data" / "feros_trace_fixtures.npz") as z:
        return {k: z[k] for k in z.files}


@pytest.fixture(scope="module")
def tmpl() -> dict:
    return F.load_trace_template()


def _shift(c: np.ndarray, d: float) -> np.ndarray:
    c = np.array(c, dtype=float, copy=True)
    c[..., -1] += d
    return c


def _pos(c: np.ndarray) -> np.ndarray:
    return np.array([np.polyval(c, x) for x in X_CHECK])


def _slot(tmpl: dict, label: str) -> int:
    return tmpl["labels"].index(label)


def _row(label: str) -> int:
    """Row of ``label`` in the 72-row layout (orders 0..35 interleaved ob, co)."""
    fib, order = label[:2], int(label[2:])
    return 2 * order + (fib == "co")


def _drop(tmpl: dict, labels: set[str], offset: float = 0.0) -> np.ndarray:
    keep = [k for k, lab in enumerate(tmpl["labels"]) if lab not in labels]
    return _shift(tmpl["coeffs"][keep], offset)


# ---------------------------------------------------------------------------
# template
# ---------------------------------------------------------------------------

def test_template_is_packaged_and_complete(tmpl):
    path = Path(tmpl["path"])
    # meson install_subdir('src/ceres3') ships everything under instruments/data
    assert path.parent.name == "data" and path.parent.parent.name == "instruments"
    assert tmpl["npix"] == NPIX
    assert tmpl["coeffs"].shape == (74, 5)
    assert list(tmpl["order"]) == [k // 2 - 1 for k in range(74)]
    assert list(tmpl["fibre"]) == ["ob", "co"] * 37
    yc = np.array([np.polyval(c, 2048) for c in tmpl["coeffs"]])
    assert np.all(np.diff(yc) > 12.0)  # 12.7 px minimum physical spacing


# ---------------------------------------------------------------------------
# label_traces
# ---------------------------------------------------------------------------

def test_healthy_night_is_the_identity(tmpl):
    c_all, info = F.label_traces(_shift(tmpl["coeffs"], 1.3), NPIX)
    assert c_all.shape == (72, 5)
    np.testing.assert_array_equal(c_all, _shift(tmpl["coeffs"][2:], 1.3))
    assert info["offset_px"] == pytest.approx(1.3, abs=1e-6)
    assert info["matched"] == 72 and info["synthesized"] == []
    assert info["dropped"] == ["ob-1", "co-1"] and info["rejected_y_px"] == []
    assert info["n_raw"] == 74


def test_healthy_legacy_c_all_is_reproduced_bit_for_bit(fx):
    """The new labelling does not perturb a healthy night, even with the weak
    co0 trace fitted 4.3 px away from its template position."""
    legacy = fx["legacy_2026-09-16"]
    c_all, info = F.label_traces(legacy, NPIX)
    np.testing.assert_array_equal(c_all, legacy)
    assert info["synthesized"] == []
    assert info["max_residual_used_px"] < 0.5
    assert 4.0 < info["max_residual_px"] < F.TRACE_OUTER_TOL


def test_0915_pattern_production_noise(fx):
    """co-1/co0 and orders 33-35 missing: the legacy [2:] shifted every order by
    one; labelled, the calibrated orders land on their physical traces."""
    raw = fx["raw_std_2026-09-15"]
    assert len(raw) == 66
    c_all, info = F.label_traces(raw, NPIX)
    assert info["synthesized"] == ["co0", "ob33", "co33", "ob34", "co34", "ob35", "co35"]
    assert info["dropped"] == ["ob-1"]
    # production c_all = raw[2:]; the investigation found it sits one order up
    np.testing.assert_array_equal(c_all[2:66], fx["legacy_2026-09-15"])
    np.testing.assert_array_equal(raw[2:], fx["legacy_2026-09-15"])


def test_0915_pattern_robust_noise(fx):
    """With the MAD noise floor only co-1/co0 stay undetected; co0 is outside the
    calibrated orders and is placed from the template."""
    c_all, info = F.label_traces(fx["raw_mad_2026-09-15"], NPIX)
    assert info["synthesized"] == ["co0"] and info["matched"] == 71
    legacy = fx["legacy_2026-09-15"]
    for k in range(len(legacy)):  # legacy row k is physical row k + 2
        assert np.abs(_pos(c_all[k + 2]) - _pos(legacy[k])).max() < 0.3
    assert info["max_residual_used_px"] < 0.5


def test_fibre_swap_pattern(fx):
    """05-14: co0 missing and co-1 fitted 4.6 px off. Legacy rows 1..62 sit one
    trace up (object = physical comparison of the same order)."""
    legacy = fx["legacy_2026-05-14"]
    # production c_all = raw[2:] (re-run here to 1e-12 px, different machine)
    assert max(np.abs(_pos(a) - _pos(b)).max() for a, b in zip(fx["raw_std_2026-05-14"][2:], legacy)) < 1e-6
    c_all, info = F.label_traces(fx["raw_mad_2026-05-14"], NPIX)
    assert info["synthesized"] == ["co0"]
    assert info["dropped"] == ["ob-1", "co-1"]
    assert np.abs(_pos(c_all[0]) - _pos(legacy[0])).max() < 0.3
    for k in range(1, 63):
        assert np.abs(_pos(c_all[k + 1]) - _pos(legacy[k])).max() < 0.3
    # the production noise estimate also lost ob32, inside the calibrated orders
    with pytest.raises(F.FerosTraceError, match="ob32"):
        F.label_traces(fx["raw_std_2026-05-14"], NPIX)


def test_fibre_swap_synthetic(tmpl):
    c_all, info = F.label_traces(_drop(tmpl, {"co0"}, -2.4), NPIX)
    assert info["synthesized"] == ["co0"]
    for j in range(36):
        assert np.abs(_pos(c_all[2 * j]) - _pos(_shift(tmpl["coeffs"][_slot(tmpl, f"ob{j}")], -2.4))).max() < 1e-6
        assert np.abs(_pos(c_all[2 * j + 1]) - _pos(_shift(tmpl["coeffs"][_slot(tmpl, f"co{j}")], -2.4))).max() < 1e-6


def test_legacy_fibre_swapped_rows_are_identified(fx, tmpl):
    """2024-04-29 (66 legacy rows): the investigation found pipeline c_ob[8] on
    physical co8. label_traces run on those rows recovers that identity."""
    legacy = fx["legacy_2024-04-29"]
    c_all, info = F.label_traces(legacy, NPIX)
    yc = np.array([np.polyval(c, 2048) for c in c_all])
    k = int(np.argmin(np.abs(yc - np.polyval(legacy[16], 2048))))
    assert k == _row("co8")
    np.testing.assert_array_equal(c_all[k], legacy[16])


def test_odd_count(fx, tmpl):
    raw = fx["raw_std_2026-07-02"]
    assert len(raw) % 2 == 1  # crashed the legacy ob/co split with an IndexError
    with pytest.raises(F.FerosTraceError, match="co32"):
        F.label_traces(raw, NPIX)
    c_all, info = F.label_traces(fx["raw_mad_2026-07-02"], NPIX)
    assert c_all.shape == (72, 5) and info["synthesized"] == ["co0"]
    c_all, info = F.label_traces(_drop(tmpl, {"co-1"}), NPIX)  # 73 traces
    np.testing.assert_array_equal(c_all, tmpl["coeffs"][2:])


def test_missing_trace_in_used_range_raises(tmpl):
    with pytest.raises(F.FerosTraceError, match=r"^FEROS trace labelling failed: .*ob20"):
        F.label_traces(_drop(tmpl, {"ob20"}, 0.7), NPIX)


def test_unexplained_trace_in_used_range_raises(tmpl):
    raw = np.vstack([tmpl["coeffs"], _shift(tmpl["coeffs"][_slot(tmpl, "co20")], 6.0)])
    with pytest.raises(F.FerosTraceError, match="does not match|compete"):
        F.label_traces(raw, NPIX)


def test_missing_outside_used_range_is_synthesised_with_the_offset(tmpl):
    missing = {"ob0", "co0", "co3", "ob34", "co35"}
    c_all, info = F.label_traces(_drop(tmpl, missing, 3.2), NPIX)
    assert set(info["synthesized"]) == missing
    for lab in missing:
        np.testing.assert_allclose(c_all[_row(lab)], _shift(tmpl["coeffs"][_slot(tmpl, lab)], info["offset_px"]),
                                   rtol=0, atol=1e-9)
    assert info["offset_px"] == pytest.approx(3.2, abs=1e-6)


def test_ambiguous_offset_raises(tmpl):
    """A lone trace midway between co0 and ob1 (12.5 px apart) is explained
    equally well by offsets of +6.25 and -6.25 px, both inside the search window."""
    mid = 0.5 * (np.polyval(tmpl["coeffs"][_slot(tmpl, "co0")], 2048) + np.polyval(tmpl["coeffs"][_slot(tmpl, "ob1")], 2048))
    c = tmpl["coeffs"][[_slot(tmpl, "co0")]]
    one = _shift(c, mid - np.polyval(c[0], 2048))
    with pytest.raises(F.FerosTraceError, match="ambiguous"):
        F.label_traces(one, NPIX)


def test_implausible_offset_raises(tmpl):
    with pytest.raises(F.FerosTraceError, match="plausible"):
        F.label_traces(_shift(tmpl["coeffs"], 11.5), NPIX)


def test_wrong_detector_format_raises(tmpl):
    with pytest.raises(F.FerosTraceError, match="dispersion columns"):
        F.label_traces(tmpl["coeffs"], 2048)


def test_error_prefix():
    assert str(F.FerosTraceError("x")) == "FEROS trace labelling failed: x"
    assert isinstance(F.FerosTraceError("x"), RuntimeError)


# ---------------------------------------------------------------------------
# legacy_trace_ok
# ---------------------------------------------------------------------------

def test_legacy_trace_ok_true(fx, tmpl):
    assert F.legacy_trace_ok(fx["legacy_2026-09-16"], NPIX) is True
    assert F.legacy_trace_ok(_shift(tmpl["coeffs"][2:], -5.5), NPIX) is True


def test_legacy_trace_ok_false(fx, tmpl):
    for night in ("2026-09-15", "2026-05-14", "2024-04-29"):
        assert F.legacy_trace_ok(fx[f"legacy_{night}"], NPIX) is False
    # 72 rows but misregistered: co0 missed and an extra blue trace found, so
    # [2:] still left 72 rows, every one of them one trace off
    extra = _shift(tmpl["coeffs"][[-1]], 60.0)
    raw = np.vstack([_drop(tmpl, {"co0"}), extra])
    assert F.legacy_trace_ok(raw[2:], NPIX) is False
    # never raises
    assert F.legacy_trace_ok(np.full((72, 5), np.nan), NPIX) is False
    assert F.legacy_trace_ok(np.zeros((3,)), NPIX) is False
    assert F.legacy_trace_ok(tmpl["coeffs"][2:], 2048) is False


# ---------------------------------------------------------------------------
# ferospipe_fp wiring (module-scope script: asserted on the parsed source)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def src() -> str:
    return PIPE.read_text()


@pytest.fixture(scope="module")
def tree(src) -> ast.Module:
    return ast.parse(src, filename=str(PIPE))


def _calls(tree: ast.AST, attr: str) -> list[ast.Call]:
    return [n for n in ast.walk(tree) if isinstance(n, ast.Call)
            and isinstance(n.func, ast.Attribute) and n.func.attr == attr]


def test_pipeline_traces_with_robust_noise_and_labels(tree):
    calls = _calls(tree, "get_them")
    assert len(calls) == 1
    kw = {k.arg: k.value for k in calls[0].keywords}
    assert isinstance(kw.get("robust_noise"), ast.Constant) and kw["robust_noise"].value is True
    assert _calls(tree, "label_traces")


def test_pipeline_no_longer_assumes_the_trace_layout(src):
    assert "c_all[2:]" not in src
    assert "np.arange(1,nord_all+1,2)" not in src
    assert "Blue coverage reduced" not in src


def test_pipeline_writes_trace_version_2(tree):
    fn = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "trace_orders")
    dicts = [n for n in ast.walk(fn) if isinstance(n, ast.Dict)]
    keys = {k.value: v for d in dicts for k, v in zip(d.keys, d.values) if isinstance(k, ast.Constant)}
    assert isinstance(keys.get("trace_version"), ast.Constant) and keys["trace_version"].value == 2
    assert "trace_info" in keys


def test_pipeline_validates_legacy_trace_pickles(tree):
    calls = _calls(tree, "legacy_trace_ok")
    assert calls, "a pre-1.2 trace.pkl is never validated"
    guard = next(n for n in ast.walk(tree) if isinstance(n, ast.If) and calls[0] in list(ast.walk(n.test)))
    body = "\n".join(ast.unparse(s) for s in guard.body)
    assert "if not is_calib:" in body and "FerosTraceError" in body and "misregistered" in body
    assert "trace_orders(" in body  # -is_calib: re-trace


def test_retrace_forces_downstream_recomputation(tree):
    blk = next(n for n in tree.body if isinstance(n, ast.If) and ast.unparse(n.test) == "retraced and is_calib")
    src = ast.unparse(blk)
    for flag in ("force_flat_extract", "force_thar_extract", "force_thar_wavcal"):
        assert f"{flag} = True" in src
    assert "shifts.pkl" in src
    # nothing resets the flags between that block and the extraction it forces
    resets = [n for n in tree.body if isinstance(n, ast.Assign) and n.lineno > blk.lineno
              and any(isinstance(t, ast.Name) and t.id.startswith("force_") for t in n.targets)]
    assert not resets, [ast.unparse(n) for n in resets]


def test_order_count_clamps_are_hard_errors(src, tree):
    assert "n_useful = thar_S_ob.shape[0] - o0" not in src
    guards = [n for n in ast.walk(tree) if isinstance(n, ast.If)
              and ("nord_ob < o0 + n_useful" in ast.unparse(n.test)
                   or "thar_S_ob.shape[0] < o0 + n_useful" in ast.unparse(n.test))]
    assert len(guards) == 2
    for g in guards:
        assert isinstance(g.body[0], ast.Raise) and "FerosTraceError" in ast.unparse(g.body[0])
