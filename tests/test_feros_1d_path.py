"""The merged 1D spectrum is written beside its _sp.fits product.

``fout`` is ``proc/<obname>_<date>_UT<time>_sp.fits``; joining ``'proc'`` onto
``dirout`` again pointed every 1D spectrum at ``<dirout>/proc/proc/``, which
nothing creates. compute_activity swallows the failed write, so no FEROS
product carried SPEC1D and the 1D spectra came only from a later post-processing
pass (EXOAUTOMAT-333).

The pipeline runs at module scope with no importable seam, so this parses the
source, as tests/test_feros_mask_card.py does.
"""
from __future__ import annotations

import ast
import os
from pathlib import Path

PIPE = (Path(__file__).resolve().parents[1]
        / "src" / "ceres3" / "instruments" / "ferospipe_fp.py")


def _assignments(name):
    tree = ast.parse(PIPE.read_text(), filename=str(PIPE))
    return [node.value for node in ast.walk(tree)
            if isinstance(node, ast.Assign)
            and any(isinstance(t, ast.Name) and t.id == name for t in node.targets)]


def test_the_1d_spectrum_lands_in_proc_not_proc_proc():
    (expr,) = _assignments("spectrum_1d_path")
    # every fout is an f-string whose literal head is "proc/"
    heads = [f.values[0].value for f in _assignments("fout") if isinstance(f, ast.JoinedStr)]
    assert heads and all(h.startswith("proc/") for h in heads)
    path = eval(compile(ast.Expression(expr), str(PIPE), "eval"),
                {"os": os, "dirout": "/night_red/", "fout": "proc/HD1_20170821_UT03-45-41.790_sp.fits"})
    assert path == "/night_red/proc/HD1_20170821_UT03-45-41.790_1d.fits"
