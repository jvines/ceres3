"""Post-processing API for computing activity indicators on reduced spectra.

Usage from Python:

    from ceres3.postprocess import process_spectrum, process_directory

    # Single file
    result = process_spectrum('/path/to/star_sp.fits', save_1d=True)

    # Whole night
    results = process_directory('/path/to/2019-12-04_red/', save_1d=True, update_fits=True)
"""

import os
import glob
import numpy as np
from astropy.io import fits as pyfits
from ceres3.utils.activity import compute_activity


def process_spectrum(fits_path, instrument=None, save_1d=False,
                     update_fits=False, teff=None):
    """Compute activity indicators for a single reduced spectrum.

    Parameters
    ----------
    fits_path : str
        Path to a CERES *_sp.fits spectrum cube.
    instrument : str, optional
        Override instrument name (default: read from FITS header).
    save_1d : bool
        Save merged 1D spectrum as *_1d.fits alongside the input.
    update_fits : bool
        Write activity values back into the FITS header.
    teff : float, optional
        Override Teff for log R'HK (default: read from FITS header).

    Returns
    -------
    dict with all activity indicator values + metadata.
    """
    hdul = pyfits.open(fits_path)
    header = hdul[0].header

    # Support four FITS layouts:
    #   (1) native ceres3 ``*_sp.fits`` — 3-D primary HDU cube.
    #   (2) pre-merged 2-D ``[wavelength, flux(, error)]`` primary HDU, as
    #       written by ExoAutomata's ``shared.spectra_prep._rest_frame_single``.
    #   (3) ``SPECTRUM_1D`` binary-table extension with WAVELENGTH / FLUX
    #       (and optionally ERROR) columns — ceres3's own 1-D output, also
    #       the format archive-flow staging writes from MinIO.
    #   (4) ESO Phase-3 s1d layout — 1-D flux in the primary HDU with
    #       wavelength derived from CRVAL1 / CDELT1 / NAXIS1. HARPS and
    #       ESPRESSO archive spectra all use this layout.
    if 'SPECTRUM_1D' in hdul:
        tbl = hdul['SPECTRUM_1D'].data
        wavelength = np.asarray(tbl['WAVELENGTH'], dtype=float)
        flux = np.asarray(tbl['FLUX'], dtype=float)
        if 'ERROR' in tbl.dtype.names:
            error = np.asarray(tbl['ERROR'], dtype=float)
            spec = np.vstack([wavelength, flux, error])
        else:
            spec = np.vstack([wavelength, flux])
    elif (
        hdul[0].data is not None
        and hdul[0].data.ndim == 1
        and 'CRVAL1' in header
        and 'CDELT1' in header
    ):
        flux = np.asarray(hdul[0].data, dtype=float)
        naxis1 = int(header.get('NAXIS1', flux.size))
        crval1 = float(header['CRVAL1'])
        cdelt1 = float(header['CDELT1'])
        wavelength = crval1 + cdelt1 * np.arange(naxis1, dtype=float)
        # s1d has no error column — compute_activity's 2-D branch falls
        # back to sqrt(flux) Poisson errors, which is the right default
        # for photon-counted ESO P3 data.
        spec = np.vstack([wavelength, flux])
    else:
        spec = hdul[0].data

    if instrument is None:
        instrument = header.get('INST', 'feros').lower()

    if teff is None:
        teff = header.get('TEFF', None)
        if teff is not None and teff <= 0:
            teff = None

    out_1d = fits_path.replace('_sp.fits', '_1d.fits') if save_1d else None

    activity = compute_activity(spec, instrument=instrument,
                                output_1d_path=out_1d, teff=teff)

    # Add metadata from header
    activity['filename'] = os.path.basename(fits_path)
    activity['bjd'] = header.get('BJD_OUT', 0.0)
    activity['rv'] = header.get('RV', 0.0)
    activity['rv_err'] = header.get('RV_E', 0.0)

    if update_fits:
        with pyfits.open(fits_path, mode='update') as hdu:
            for key, val in activity.items():
                if key in ('filename', 'spectrum_1d_path'):
                    continue
                try:
                    hdu[0].header[key.upper()] = np.around(val, 6)
                except Exception:
                    pass
            if activity.get('spectrum_1d_path'):
                hdu[0].header['SPEC1D'] = activity['spectrum_1d_path']
            hdu.flush()

    hdul.close()
    return activity


def process_directory(input_dir, instrument=None, save_1d=False,
                      update_fits=False):
    """Compute activity indicators for all reduced spectra in a directory.

    Parameters
    ----------
    input_dir : str
        Path to reduction output directory (looks for proc/*_sp.fits).
    instrument : str, optional
        Override instrument name.
    save_1d : bool
        Save merged 1D spectra.
    update_fits : bool
        Write activity values back into FITS headers.

    Returns
    -------
    list of dicts, one per spectrum.
    """
    input_dir = input_dir.rstrip('/')
    proc_dir = os.path.join(input_dir, 'proc')
    if not os.path.isdir(proc_dir):
        proc_dir = input_dir

    fits_files = sorted(glob.glob(os.path.join(proc_dir, '*_sp.fits')))
    if not fits_files:
        raise FileNotFoundError(f"No *_sp.fits files found in {proc_dir}")

    results = []
    for fpath in fits_files:
        try:
            result = process_spectrum(fpath, instrument=instrument,
                                      save_1d=save_1d, update_fits=update_fits)
            results.append(result)
        except Exception as e:
            results.append({'filename': os.path.basename(fpath), 'error': str(e)})

    return results
