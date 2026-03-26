"""
Chromospheric activity indicators computed inline from echelle spectra.

Computes S-index (Ca H+K), H-alpha, HeI, and NaI D1/D2 indices directly
from the spec array in memory — no FITS I/O round-trip needed.

Algorithms and constants match Ceres-plusplus (cerespp) exactly.
"""

import numpy as np
from scipy.interpolate import interp1d
from scipy.integrate import trapezoid
from scipy.signal.windows import triang

# ── Line centers (Angstroms) ──────────────────────────────────────────────
CaK = 3933.664
CaH = 3968.47
CaV = 3901.0
CaR = 4001.0
Ha = 6562.808
Hb = 4861.327
HeI = 5875.62
NaID1 = 5895.92
NaID2 = 5889.95
CaIRT1 = 8498.02
CaIRT2 = 8542.09
CaIRT3 = 8662.14
MgIb1 = 5167.321
MgIb2 = 5172.684
MgIb3 = 5183.604

# Instruments that can't compute S-index (wavelength coverage too blue)
S_INDEX_EXCEPTIONS = {'fideos'}
# Instruments that can't compute Ca II IRT (wavelength coverage doesn't reach ~8700 A)
CA_IRT_EXCEPTIONS = {'feros', 'fideos', 'cafe', 'pucheros'}


def _teff_to_bv(teff):
    """Approximate B-V from Teff using Sekiguchi & Fukugita (2000).
    Valid for ~4000-7000 K main sequence stars."""
    # Polynomial inversion of Teff(B-V) relation
    # Teff = 8540 / (B-V + 0.865) for solar metallicity (ballpark)
    return 8540.0 / teff - 0.865


def _noyes_ccf(bv):
    """Noyes et al. (1984) Ccf bolometric correction factor.
    log Ccf = polynomial in (B-V)."""
    x = bv
    if x < 0.63:
        return 1.13 - 10.918 * x + 36.861 * x**2 - 45.648 * x**3 + 18.0 * x**4
    else:
        return -2.117 + 8.204 * x - 8.446 * x**2 + 3.189 * x**3


def _r_phot(bv):
    """Photospheric contribution to R_HK (Noyes et al. 1984).
    log R_phot = polynomial in (B-V)."""
    x = bv
    log_rphot = -4.898 + 1.918 * x**2 - 2.893 * x**3
    return 10**log_rphot


def _get_response(line, width, filt='square'):
    """Build a response filter function centered on a spectral line."""
    if filt == 'square':
        hw = width / 2.0
        w_resp = np.linspace(line - hw, line + hw, 1000)
        resp_f = interp1d(w_resp, np.ones(1000),
                          bounds_error=False, fill_value=0.0)
        return resp_f, hw
    elif filt == 'triangle':
        w_resp = np.linspace(line - width, line + width, 999)
        resp_f = interp1d(w_resp, triang(999, True),
                          bounds_error=False, fill_value=0.0)
        return resp_f, width


def _get_ini_end(wavelength, line, width):
    """Find index range covering line ± width."""
    ini = max(0, np.searchsorted(wavelength, line - width) - 2)
    end = min(len(wavelength), np.searchsorted(wavelength, line + width) + 2)
    return ini, end


def get_line_flux(wavelength, flux, line, width, filt='square', error=None):
    """Compute integrated line flux convolved with a response filter.

    Parameters
    ----------
    wavelength : array — merged 1D wavelength array (Angstroms)
    flux : array — merged 1D flux array
    line : float — line center (Angstroms)
    width : float — bandwidth (full width for square, FWHM for triangle)
    filt : str — 'square' or 'triangle'
    error : array, optional — flux error array

    Returns
    -------
    integrated_flux : float
    sigma : float (only if error is provided)
    """
    resp_f, hw = _get_response(line, width, filt)
    ini, end = _get_ini_end(wavelength, line, hw)

    if end - ini < 4:
        if error is not None:
            return -999.0, -999.0
        return -999.0

    n_pts = max(end - ini, 10)
    w = np.linspace(line - hw, line + hw, n_pts)

    # Interpolate flux onto uniform grid within the line region
    pad = 2
    sl = slice(max(0, ini - pad), min(len(wavelength), end + pad))
    intp = interp1d(wavelength[sl], flux[sl],
                    bounds_error=False, fill_value='extrapolate')
    intp_flux = intp(w)
    response = resp_f(w)

    denom = trapezoid(response, w)
    if denom == 0:
        if error is not None:
            return -999.0, -999.0
        return -999.0

    integrated_flux = trapezoid(intp_flux * response, w) / denom

    if error is not None:
        err_slice = error[ini:end]
        wav_slice = wavelength[ini:end]
        resp_vals = resp_f(wav_slice)
        num = np.sum((err_slice * resp_vals) ** 2)
        den = np.sum(resp_vals ** 2)
        sigma = np.sqrt(num / den) if den > 0 else -999.0
        return integrated_flux, sigma

    return integrated_flux


def merge_echelle(spec, wav_idx=0, flux_idx=5, err_idx=6, snr_idx=8):
    """Merge overlapping echelle orders into a single 1D spectrum.

    Uses S/N crossover between adjacent orders to determine the optimal
    stitching wavelength.

    Parameters
    ----------
    spec : ndarray, shape (n_layers, n_orders, n_pix)
    wav_idx, flux_idx, err_idx, snr_idx : layer indices

    Returns
    -------
    wavelength, flux, error : 1D arrays
    """
    n_orders = spec.shape[1]
    wave_parts, flux_parts, err_parts = [], [], []
    next_start = 0

    for i in range(n_orders - 1, 0, -1):
        wc = spec[wav_idx, i, :]
        wn = spec[wav_idx, i - 1, :]
        snc = spec[snr_idx, i, :]
        snn = spec[snr_idx, i - 1, :]

        # Find S/N crossover point in the overlap region
        overlap_lo = max(wn[0], wc[0])
        overlap_hi = min(wn[-1], wc[-1])

        if overlap_hi <= overlap_lo:
            # No overlap — use the full order
            cur_end = len(wc)
        else:
            wav_grid = np.linspace(overlap_lo, overlap_hi, 1000)
            sn_cur = np.interp(wav_grid, wc, snc)
            sn_nxt = np.interp(wav_grid, wn, snn)
            crossover_wav = wav_grid[np.argmin(np.abs(sn_cur - sn_nxt))]
            cur_end = min(np.searchsorted(wc, crossover_wav), len(wc) - 1)

        wave_parts.append(spec[wav_idx, i, next_start:cur_end])
        flux_parts.append(spec[flux_idx, i, next_start:cur_end])
        err_parts.append(spec[err_idx, i, next_start:cur_end])

        if overlap_hi > overlap_lo:
            next_start = min(np.searchsorted(wn, crossover_wav), len(wn) - 1)
        else:
            next_start = 0

    # Final (bluest) order
    wave_parts.append(spec[wav_idx, 0, next_start:])
    flux_parts.append(spec[flux_idx, 0, next_start:])
    err_parts.append(spec[err_idx, 0, next_start:])

    wavelength = np.concatenate(wave_parts)
    flux = np.concatenate(flux_parts)
    error = np.concatenate(err_parts)

    return wavelength, flux, error


def compute_activity(spec, instrument='feros', output_1d_path=None, teff=None):
    """Compute all activity indicators from an echelle spectrum array.

    Parameters
    ----------
    spec : ndarray, shape (11, n_orders, n_pix)
        The standard CERES output spectrum array.
        Layer 0: wavelength, 5: normalized flux, 6: errors, 8: SNR.
    instrument : str
        Instrument name (used to skip indicators outside wavelength coverage).
    output_1d_path : str, optional
        If provided, save the merged 1D rest-frame spectrum as a FITS file.
    teff : float, optional
        Effective temperature in K. Used to compute log R'HK from S-index.

    Returns
    -------
    dict with keys: s_index, s_index_err, log_rhk, halpha, halpha_err,
                    hbeta, hbeta_err, hei, hei_err, nai_d1d2, nai_d1d2_err,
                    ca_irt, ca_irt_err, mg_ib, mg_ib_err, spectrum_1d_path
    """
    result = {
        's_index': -999.0, 's_index_err': -999.0,
        'log_rhk': -999.0,
        'halpha': -999.0, 'halpha_err': -999.0,
        'hbeta': -999.0, 'hbeta_err': -999.0,
        'hei': -999.0, 'hei_err': -999.0,
        'nai_d1d2': -999.0, 'nai_d1d2_err': -999.0,
        'ca_irt1': -999.0, 'ca_irt1_err': -999.0,
        'ca_irt2': -999.0, 'ca_irt2_err': -999.0,
        'ca_irt3': -999.0, 'ca_irt3_err': -999.0,
        'mg_ib': -999.0, 'mg_ib_err': -999.0,
    }

    # Merge echelle orders into 1D spectrum
    wavelength, flux, error = merge_echelle(spec)

    # Remove bad pixels
    good = (wavelength > 0) & np.isfinite(flux) & np.isfinite(error)
    wavelength, flux, error = wavelength[good], flux[good], error[good]

    if len(wavelength) < 100:
        return result

    # Save merged 1D spectrum if requested
    result['spectrum_1d_path'] = None
    if output_1d_path is not None:
        try:
            from astropy.io import fits as pyfits
            col_wav = pyfits.Column(name='WAVELENGTH', format='D', array=wavelength, unit='Angstrom')
            col_flx = pyfits.Column(name='FLUX', format='D', array=flux)
            col_err = pyfits.Column(name='ERROR', format='D', array=error)
            hdu = pyfits.BinTableHDU.from_columns([col_wav, col_flx, col_err])
            hdu.header['EXTNAME'] = 'SPECTRUM_1D'
            primary = pyfits.PrimaryHDU()
            hdul = pyfits.HDUList([primary, hdu])
            hdul.writeto(output_1d_path, overwrite=True)
            result['spectrum_1d_path'] = output_1d_path
        except Exception:
            pass

    # ── S-index (Ca H+K) ─────────────────────────────────────────────
    if instrument.lower() not in S_INDEX_EXCEPTIONS:
        try:
            NV, sNV = get_line_flux(wavelength, flux, CaV, 20, 'square', error)
            NR, sNR = get_line_flux(wavelength, flux, CaR, 20, 'square', error)
            NK, sNK = get_line_flux(wavelength, flux, CaK, 1.09, 'triangle', error)
            NH, sNH = get_line_flux(wavelength, flux, CaH, 1.09, 'triangle', error)

            if all(v != -999.0 for v in [NV, NR, NK, NH]) and (NR + NV) != 0:
                S = (NH + NK) / (NR + NV)
                sS_num = np.sqrt(sNH**2 + sNK**2)
                sS_den = np.sqrt(sNV**2 + sNR**2)
                sS = S * np.sqrt((sS_num / (NH + NK))**2 + (sS_den / (NR + NV))**2)
                result['s_index'] = float(S)
                result['s_index_err'] = float(sS)
        except Exception:
            pass

    # ── log R'HK (Noyes et al. 1984 calibration from S-index) ────────
    if result['s_index'] != -999.0 and teff is not None and teff > 0:
        try:
            # B-V from Teff using Sekiguchi & Fukugita (2000) approximation
            # Valid for 4000 < Teff < 7000 K
            if 4000 < teff < 7000:
                bv = _teff_to_bv(teff)
                # Noyes et al. 1984: log R'HK from S-index and B-V
                log_ccf = _noyes_ccf(bv)
                r_phot = _r_phot(bv)
                rhk = 1.340e-4 * 10**log_ccf * result['s_index']
                r_prime_hk = rhk - r_phot
                if r_prime_hk > 0:
                    result['log_rhk'] = float(np.log10(r_prime_hk))
        except Exception:
            pass

    # ── H-alpha ───────────────────────────────────────────────────────
    try:
        FHa, sFHa = get_line_flux(wavelength, flux, Ha, 0.678, 'square', error)
        F1, sF1 = get_line_flux(wavelength, flux, 6550.87, 10.75, 'square', error)
        F2, sF2 = get_line_flux(wavelength, flux, 6580.309, 8.75, 'square', error)

        if all(v != -999.0 for v in [FHa, F1, F2]) and (F1 + F2) != 0:
            Halpha = FHa / (0.5 * (F1 + F2))
            sden = np.sqrt(sF1**2 + sF2**2)
            sHalpha = Halpha * np.sqrt((sFHa / FHa)**2 + (sden / (F1 + F2))**2)
            result['halpha'] = float(Halpha)
            result['halpha_err'] = float(sHalpha)
    except Exception:
        pass

    # ── HeI ───────────────────────────────────────────────────────────
    try:
        FHeI, sFHeI = get_line_flux(wavelength, flux, HeI, 0.2, 'square', error)
        F1, sF1 = get_line_flux(wavelength, flux, 5874.5, 0.5, 'square', error)
        F2, sF2 = get_line_flux(wavelength, flux, 5879.0, 0.5, 'square', error)

        if all(v != -999.0 for v in [FHeI, F1, F2]) and (F1 + F2) != 0:
            HelI = FHeI / (0.5 * (F1 + F2))
            sden = np.sqrt(sF1**2 + sF2**2)
            sHelI = HelI * np.sqrt((sFHeI / FHeI)**2 + (sden / (F1 + F2))**2)
            result['hei'] = float(HelI)
            result['hei_err'] = float(sHelI)
    except Exception:
        pass

    # ── NaI D1/D2 ────────────────────────────────────────────────────
    try:
        D1, sD1 = get_line_flux(wavelength, flux, NaID1, 1.0, 'square', error)
        D2, sD2 = get_line_flux(wavelength, flux, NaID2, 1.0, 'square', error)
        L, sL = get_line_flux(wavelength, flux, 5805.0, 10.0, 'square', error)
        R, sR = get_line_flux(wavelength, flux, 6090.0, 20.0, 'square', error)

        if all(v != -999.0 for v in [D1, D2, L, R]) and (L + R) != 0:
            NaI = (D1 + D2) / (L + R)
            sNa_num = np.sqrt(sD1**2 + sD2**2)
            sNa_den = np.sqrt(sL**2 + sR**2)
            sNaI = NaI * np.sqrt((sNa_num / (D1 + D2))**2 + (sNa_den / (L + R))**2)
            result['nai_d1d2'] = float(NaI)
            result['nai_d1d2_err'] = float(sNaI)
    except Exception:
        pass

    # ── H-beta ────────────────────────────────────────────────────────
    try:
        FHb, sFHb = get_line_flux(wavelength, flux, Hb, 0.678, 'square', error)
        F1, sF1 = get_line_flux(wavelength, flux, 4847.0, 10.0, 'square', error)
        F2, sF2 = get_line_flux(wavelength, flux, 4876.0, 10.0, 'square', error)

        if all(v != -999.0 for v in [FHb, F1, F2]) and (F1 + F2) != 0:
            Hbeta = FHb / (0.5 * (F1 + F2))
            sden = np.sqrt(sF1**2 + sF2**2)
            sHbeta = Hbeta * np.sqrt((sFHb / FHb)**2 + (sden / (F1 + F2))**2)
            result['hbeta'] = float(Hbeta)
            result['hbeta_err'] = float(sHbeta)
    except Exception:
        pass

    # ── Ca II IRT (infrared triplet: 8498, 8542, 8662 A) ────────────
    # Individual line indices normalized by continuum on each side
    if instrument.lower() not in CA_IRT_EXCEPTIONS:
        try:
            CL, sCL = get_line_flux(wavelength, flux, 8475.0, 10.0, 'square', error)
            CR, sCR = get_line_flux(wavelength, flux, 8700.0, 10.0, 'square', error)

            if all(v != -999.0 for v in [CL, CR]) and (CL + CR) != 0:
                cont = 0.5 * (CL + CR)
                scont = 0.5 * np.sqrt(sCL**2 + sCR**2)

                for line, key in [(CaIRT1, 'ca_irt1'), (CaIRT2, 'ca_irt2'), (CaIRT3, 'ca_irt3')]:
                    Fl, sFl = get_line_flux(wavelength, flux, line, 1.0, 'square', error)
                    if Fl != -999.0 and cont != 0:
                        idx = Fl / cont
                        sidx = idx * np.sqrt((sFl / Fl)**2 + (scont / cont)**2)
                        result[key] = float(idx)
                        result[key + '_err'] = float(sidx)
        except Exception:
            pass

    # ── Mg I b triplet (5167, 5173, 5184 A) ─────────────────────────
    try:
        Fb1, sFb1 = get_line_flux(wavelength, flux, MgIb1, 0.5, 'square', error)
        Fb2, sFb2 = get_line_flux(wavelength, flux, MgIb2, 0.5, 'square', error)
        Fb3, sFb3 = get_line_flux(wavelength, flux, MgIb3, 0.5, 'square', error)
        CL, sCL = get_line_flux(wavelength, flux, 5140.0, 10.0, 'square', error)
        CR, sCR = get_line_flux(wavelength, flux, 5200.0, 10.0, 'square', error)

        if all(v != -999.0 for v in [Fb1, Fb2, Fb3, CL, CR]) and (CL + CR) != 0:
            MgI = (Fb1 + Fb2 + Fb3) / (1.5 * (CL + CR))
            sMgI_num = np.sqrt(sFb1**2 + sFb2**2 + sFb3**2)
            sMgI_den = np.sqrt(sCL**2 + sCR**2)
            sMgI = MgI * np.sqrt((sMgI_num / (Fb1 + Fb2 + Fb3))**2 + (sMgI_den / (CL + CR))**2)
            result['mg_ib'] = float(MgI)
            result['mg_ib_err'] = float(sMgI)
    except Exception:
        pass

    return result
