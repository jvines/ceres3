"""Regression tests for 1-D input path in ``ceres3.utils.activity.compute_activity``.

The archive branch of ExoAutomata's ``/api/cerespp/process/rvs`` endpoint feeds
``compute_activity`` a pre-merged 1-D spectrum (either a 2-D ``[wavelength, flux]``
primary HDU written by ``shared.spectra_prep._rest_frame_single`` or a ceres3
``SPECTRUM_1D`` binary-table extension). The original implementation assumed
3-D input and crashed at ``merge_echelle`` with ``too many indices for array``.
These tests lock in the three accepted input shapes, verify they produce the
same results, and pin down the error behaviour for obviously-malformed input.
"""
from __future__ import annotations

import numpy as np
import pytest
from astropy.io import fits

from ceres3.postprocess import process_spectrum
from ceres3.utils.activity import CaK, Ha, NaID1, compute_activity


N_LAYERS = 11


def _synthetic_1d_spectrum() -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Build a synthetic 1-D rest-frame spectrum covering Ca H&K → Na D."""
    wavelength = np.arange(3800.0, 6700.0, 0.05)
    flux = np.ones_like(wavelength)
    # Seed a few absorption-like dips at the lines compute_activity reads so
    # the result dict carries real numbers rather than -999 sentinels.
    for center, depth, sigma in (
        (CaK, 0.5, 0.7),
        (3968.47, 0.5, 0.7),  # CaH
        (Ha, 0.6, 0.3),
        (NaID1, 0.4, 0.3),
        (5889.95, 0.4, 0.3),  # NaID2
    ):
        flux -= depth * np.exp(-0.5 * ((wavelength - center) / sigma) ** 2)
    error = np.sqrt(np.clip(flux, 0.01, None))  # Poisson-like errors
    return wavelength, flux, error


def _pack_3d_cube(
    wavelength: np.ndarray, flux: np.ndarray, error: np.ndarray,
) -> np.ndarray:
    """Reshape a single-order spectrum into the ceres3 3-D ``_sp.fits`` layout.

    Single-order cube with shape ``(N_LAYERS, 1, n_pix)`` — ``merge_echelle``
    handles the 1-order degenerate case cleanly (no overlap stitching).
    """
    n_pix = wavelength.size
    cube = np.zeros((N_LAYERS, 1, n_pix))
    cube[0, 0, :] = wavelength
    cube[5, 0, :] = flux
    cube[6, 0, :] = error
    cube[8, 0, :] = flux / np.clip(error, 1e-9, None)  # SNR
    return cube


@pytest.fixture
def synthetic_spectrum() -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    return _synthetic_1d_spectrum()


# ── compute_activity ──────────────────────────────────────────────────────


def test_3d_input_still_works(synthetic_spectrum) -> None:
    """Baseline: the legacy 3-D path still returns real activity values."""
    wavelength, flux, error = synthetic_spectrum
    cube = _pack_3d_cube(wavelength, flux, error)

    result = compute_activity(cube, instrument="feros")

    assert result["s_index"] != -999.0
    assert result["halpha"] != -999.0
    assert result["nai_d1d2"] != -999.0


def test_2d_array_matches_3d_post_merge(synthetic_spectrum) -> None:
    """A 2-D ``[wavelength, flux, error]`` primary HDU must produce the same
    activity indicators as the 3-D cube that would merge to the same
    1-D spectrum."""
    wavelength, flux, error = synthetic_spectrum
    cube = _pack_3d_cube(wavelength, flux, error)
    arr2d = np.vstack([wavelength, flux, error])

    result_3d = compute_activity(cube, instrument="feros")
    result_2d = compute_activity(arr2d, instrument="feros")

    for key in ("s_index", "halpha", "nai_d1d2", "hbeta", "mg_ib"):
        assert result_2d[key] == pytest.approx(result_3d[key], rel=1e-6), key


def test_2d_array_without_error_falls_back_to_poisson(synthetic_spectrum) -> None:
    """``[wavelength, flux]`` (no error row) still returns real values."""
    wavelength, flux, _ = synthetic_spectrum
    arr2d = np.vstack([wavelength, flux])

    result = compute_activity(arr2d, instrument="feros")

    assert result["s_index"] != -999.0
    assert result["halpha"] != -999.0


def test_bad_input_raises_valueerror(synthetic_spectrum) -> None:
    """A 1-D array (single column) is ambiguous; reject cleanly rather than
    letting it propagate a cryptic IndexError from deep in the pipeline."""
    wavelength, _, _ = synthetic_spectrum
    with pytest.raises(ValueError):
        compute_activity(wavelength, instrument="feros")


# ── process_spectrum (FITS sniffing) ──────────────────────────────────────


def test_process_spectrum_reads_2d_primary_hdu(tmp_path, synthetic_spectrum) -> None:
    """2-D primary HDU ``[wavelength, flux]`` — the shape ExoAutomata's
    archive flow writes via ``shared.spectra_prep._rest_frame_single``."""
    wavelength, flux, _ = synthetic_spectrum
    path = tmp_path / "archive_2d.fits"
    hdu = fits.PrimaryHDU(data=np.vstack([wavelength, flux]))
    hdu.header["INST"] = "feros"
    hdu.writeto(path, overwrite=True)

    result = process_spectrum(str(path), save_1d=False, update_fits=False)

    assert result["s_index"] != -999.0
    assert result["halpha"] != -999.0


def test_process_spectrum_reads_spectrum_1d_bintable(
    tmp_path, synthetic_spectrum,
) -> None:
    """Ceres3 ``SPECTRUM_1D`` binary-table layout (WAVELENGTH / FLUX / ERROR)."""
    wavelength, flux, error = synthetic_spectrum
    path = tmp_path / "archive_bintable.fits"
    col_w = fits.Column(name="WAVELENGTH", format="D", array=wavelength)
    col_f = fits.Column(name="FLUX", format="D", array=flux)
    col_e = fits.Column(name="ERROR", format="D", array=error)
    hdu = fits.BinTableHDU.from_columns([col_w, col_f, col_e])
    hdu.header["EXTNAME"] = "SPECTRUM_1D"
    primary = fits.PrimaryHDU()
    primary.header["INST"] = "feros"
    fits.HDUList([primary, hdu]).writeto(path, overwrite=True)

    result = process_spectrum(str(path), save_1d=False, update_fits=False)

    assert result["s_index"] != -999.0
    assert result["halpha"] != -999.0


def test_process_spectrum_reads_3d_cube_unchanged(tmp_path, synthetic_spectrum) -> None:
    """The native ceres3 3-D ``*_sp.fits`` path must remain unaffected."""
    wavelength, flux, error = synthetic_spectrum
    cube = _pack_3d_cube(wavelength, flux, error)
    path = tmp_path / "native_sp.fits"
    hdu = fits.PrimaryHDU(data=cube)
    hdu.header["INST"] = "feros"
    hdu.writeto(path, overwrite=True)

    result = process_spectrum(str(path), save_1d=False, update_fits=False)

    assert result["s_index"] != -999.0
    assert result["halpha"] != -999.0
