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
    spec = hdul[0].data
    header = hdul[0].header

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
