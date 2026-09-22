import matplotlib
matplotlib.use("Agg")
from astropy.io import fits as pyfits
import numpy as np
from numpy import median, sqrt, array, exp
import scipy
from scipy import signal, special, optimize, interpolate
import scipy.special as sp
import copy
import glob
import os
import matplotlib.pyplot as plt
import sys
from ceres3.utils import globalutils as GLOBALutils


import statsmodels.api as sm
lowess = sm.nonparametric.lowess

def gauss2(params, x):
    amp1 = params[0]
    amp2 = params[1]
    med1 = params[2]
    med2 = params[3]
    sig1 = params[4]
    sig2 = params[5]
    g1 = amp1 * np.exp(-0.5*((x-med1)/sig1)**2)
    g2 = amp2 * np.exp(-0.5*((x-med2)/sig2)**2)
    return g1 + g2

def res_gauss2(params, g, x):
    return g-gauss2(params, x)

def get_RG(h):
    if h['HIERARCH ESO DET READ MODE'] == 'normal':
        ron = 5.1
        gain = 3.2
    elif h['HIERARCH ESO DET READ MODE'] == 'slow':
        ron = 3.0
        gain = 1.0
    else:
        ron = 7.2
        gain = 2.7
    return ron, gain

def MedianCombine(ImgList, zero_bo=False, zero='MasterBias.fits'):
    """
    Median combine a list of images
    """
    if zero_bo:
        BIAS = pyfits.getdata(zero)

    n = len(ImgList)
    if n==0:
        raise ValueError("empty list provided!")

    h = pyfits.open(ImgList[0])[0]
    d = h.data
    d = OverscanTrim(d)
    d = b_col(d)
    if zero_bo:
        d -= BIAS
    d = np.round(d).astype('int')

    factor = 1.25
    if (n < 3):
        factor = 1

    #ronoise = factor * h.header['HIERARCH ESO CORA CCD RON'] / np.sqrt(n)
    #gain    = h.header['HIERARCH ESO CORA CCD GAIN']
    ronoise, gain = get_RG(h.header)
    ronoise = ronoise/np.sqrt(n)

    if (n == 1):
        return d, ronoise, gain
    else:
        for i in range(n-1):
            #print(i)
            h = pyfits.open(ImgList[i+1])[0]
            ot = OverscanTrim(h.data)
            ot = b_col(ot)
            if zero_bo:
                d = np.dstack((d,np.round((ot - BIAS)).astype('int')))
            else:
                d = np.dstack((d,np.round(ot).astype('int')))
        return np.median(d,axis=2), ronoise, gain

def OverscanTrim(d):
    """
    Overscan correct and Trim a refurbished FEROS image
    """
    ps = d[:,:48]
    os = d[:,-48:]
    s = 0.5*(np.median(ps,axis=1)+np.median(os,axis=1))
    c = np.polyfit(np.arange(len(s)),s,4)
    overscan = np.polyval(c,np.arange(len(s)))
    newdata = np.zeros(d[:,50:-50].shape)
    for i in range(len(overscan)):
        newdata[i,:] = d[i,50:-50] - overscan[i]
    return newdata

def b_col(d):

    d2 = np.zeros(d.shape)
    ps = [[1675.,4097., 219.],\
          [1675.,1780., 222.],\
          [   0.,4097., 320.],\
          [   0.,4097., 326.],\
          [1616.,1617., 334.],\
          [1617.,1641., 334.],\
          [1641.,1696., 335.],\
          [1696.,4097., 335.],\
          [1617.,1641., 338.],\
          [1622.,1741., 342.],\
          [1622.,1741., 343.],\
          [ 868.,4097., 646.],\
          [1514.,2101., 843.],\
          [1501.,1691., 857.],\
          [1501.,1691., 858.],\
          [1475.,1681., 883.],\
          [1459.,4097., 900.],\
          [1454.,1501., 916.],\
          [1404.,4907.,1062.],\
          [1404.,4907.,1063.],\
          [1404.,4907.,1064.],\
          [ 608.,4097.,1298.],\
          [   0., 608.,1299.],\
          [ 608.,4097.,1299.]]
    ps = np.array(ps).astype('int')
    for i in range(len(ps)):
        d2[ps[i][0]:ps[i][1],ps[i][2]] = 1
    ej = np.arange(d.shape[1])
    for i in range(d.shape[0]):
        vec = d[i]
        I = np.where(d2[i]==0)[0]
        I2 = np.where(d2[i]==1)[0]
        if len(I2)>0:
            tck = scipy.interpolate.splrep(ej[I],vec[I],k=1)
            d[i] = scipy.interpolate.splev(ej,tck)

    return d

def gauss(params, x):
    med = params[0]
    sig = params[1]
    g = np.exp(-0.5*(x-med)*(x-med)/(sig*sig))
    return g

def res_gauss(params, g, x):
    return g-gauss(params, x)

def hasFP(h):
    mjd,mjd0 = mjd_fromheader(h)
    if h[0].header['HIERARCH ESO DPR TYPE'] == 'WAVE' or h[0].header['HIERARCH ESO DPR TYPE'] == 'OBJECT,WAVE':
        if mjd > 58448:
            if h[0].header['HIERARCH ESO INS CALMIRR2 ID'] == 'LAMP1' and 'ESO INS LAMP1 SWSIM' in h[0].header.keys():
                return True
    return False

def FileClassify(diri, log, lamp='LAMP3', dir_calib=None):
    """
    Classifies all files in a directory and writes a night log of science images
    """

    # define output lists
    simThAr_sci = []
    simSky_sci  = []
    biases      = []
    bias_dates  = []
    flat_dates  = []
    flats       = []
    ThArNe_ref  = []
    ThAr_Ne_ref = []
    ThArNe_ref_dates  = []
    ThAr_Ne_ref_dates = []
    darks       = []
    dark_times  = []
    ThAr_FP     = []
    FP_FP       = []
    FP_sci      = []

    f = open(log,'w')
    bad_files = []
    if os.access(diri+'bad_files.txt',os.F_OK):
        bf = open(diri+'bad_files.txt')
        linesbf = bf.readlines()
        for line in linesbf:
            bad_files.append(diri+line[:-1])
        bf.close()

    if dir_calib is not None:
        if os.access(dir_calib+'bad_files.txt',os.F_OK):
            bf = open(dir_calib+'bad_files.txt')
            linesbf = bf.readlines()
            for line in linesbf:
                bad_files.append(dir_calib+line[:-1])
            bf.close()

    all_files = glob.glob(diri+"*fits")
    if dir_calib is not None:
        all_files += glob.glob(dir_calib+"*fits")

    jj = 0
    for archivo in all_files:

        jj+=1
        dump = False
        for bf in bad_files:
            if archivo == bf:
                dump = True
                break

        if not dump:
            h = pyfits.open(archivo)

            print(archivo, h[0].header['HIERARCH ESO DPR TYPE'])
            mjd,mjd0 = mjd_fromheader(h)
            if h[0].header['HIERARCH ESO DPR TYPE'] == 'OBJECT,WAVE' or h[0].header['HIERARCH ESO DPR TYPE'] == 'VELOC,WAVE':
                if mjd < 58448:
                    simThAr_sci.append(archivo)
                else:
                    #if h[0].header['HIERARCH ESO INS CALMIRR2 ID'] == 'LAMP1' and 'ESO INS LAMP1 SWSIM' in h[0].header.keys():
                     #   FP_sci.append(archivo)
                    #elif h[0].header['HIERARCH ESO INS CALMIRR2 ID'] == 'LAMP3':
                    simThAr_sci.append(archivo)
                obname = h[0].header['OBJECT']
                ra     = h[0].header['HIERARCH ESO INS ADC1 RA']
                delta  = h[0].header['HIERARCH ESO INS ADC1 DEC']
                try:
                    airmass= h[0].header['HIERARCH ESO TEL AIRM START']
                except:
                    airmass = -999.
                texp   = h[0].header['EXPTIME']
                date   = h[0].header['DATE-OBS']
                line = "%-15s %10s %10s %8.2f %4.2f %s %8s %s\n" % (obname, ra, delta, texp, airmass, h[0].header['HIERARCH ESO DPR TYPE'], date, archivo)
                f.write(line)
            elif h[0].header['HIERARCH ESO DPR TYPE'] == 'OBJECT,SKY' or h[0].header['HIERARCH ESO DPR TYPE'] == 'VELOC,SKY':
                simSky_sci.append(archivo)
                obname = h[0].header['OBJECT']
                ra     = h[0].header['HIERARCH ESO INS ADC1 RA']
                delta  = h[0].header['HIERARCH ESO INS ADC1 DEC']
                try:
                    airmass= h[0].header['HIERARCH ESO TEL AIRM START']
                except:
                    airmass = -999.
                texp   = h[0].header['EXPTIME']
                date   = h[0].header['DATE-OBS']
                line = "%-15s %10s %10s %8.2f %4.2f %s %8s %s\n" % (obname, ra, delta, texp, airmass, h[0].header['HIERARCH ESO DPR TYPE'], date, archivo)
                f.write(line)

            elif h[0].header['HIERARCH ESO DPR TYPE'] == 'BIAS':
                biases.append(archivo)
                bias_dates.append(mjd)

            elif h[0].header['HIERARCH ESO DPR TYPE'] == 'FLAT':
                flats.append(archivo)
                flat_dates.append(mjd)

            elif h[0].header['HIERARCH ESO DPR TYPE'] == 'WAVE':
                if mjd > 58448:
                    if h[0].header['HIERARCH ESO INS CALMIRR2 ID'] == 'LAMP1' and 'ESO INS LAMP1 SWSIM' in h[0].header.keys():
                        FP_FP.append(archivo)
                    elif h[0].header['HIERARCH ESO INS CALMIRR2 ID'] == 'LAMP1':
                        ThAr_FP.append(archivo)
                    elif h[0].header['HIERARCH ESO INS CALMIRR2 ID'] == 'LAMP3':
                        ThAr_Ne_ref.append(archivo)
                        ThAr_Ne_ref_dates.append( mjd )
                else:
                    if h[0].header['HIERARCH ESO INS CALMIRR2 ID'] == lamp:
                        ThArNe_ref.append(archivo)
                        ThArNe_ref_dates.append( mjd )

                    if h[0].header['HIERARCH ESO INS CALMIRR2 ID'] == lamp:
                        ThAr_Ne_ref.append(archivo)
                        ThAr_Ne_ref_dates.append( mjd )

            elif h[0].header['HIERARCH ESO DPR TYPE'] == 'DARK':
                darks.append(archivo)
                dark_times.append(h[0].header['EXPTIME'])

    f.close()
    biases, bias_dates = np.array(biases), np.array(bias_dates)
    flats, flat_dates  = np.array(flats), np.array(flat_dates)
    darks, dark_times  = np.array(darks), np.array(dark_times)
    FP_FP, ThAr_FP, FP_sci = np.array(FP_FP), np.array(ThAr_FP), np.array(FP_sci)
    IS = np.argsort(bias_dates)
    biases, bias_dates = biases[IS], bias_dates[IS]
    IS = np.argsort(flat_dates)
    flats, flat_dates = flats[IS], flat_dates[IS]

    return biases, flats, ThArNe_ref, ThAr_Ne_ref, simThAr_sci, simSky_sci, ThArNe_ref_dates, ThAr_Ne_ref_dates, darks, dark_times, ThAr_FP, FP_FP, FP_sci

def mjd_fromheader(h):
    """
    return modified Julian date from header
    """

    datetu = h[0].header['DATE-OBS']
    if len(datetu) < 23:
        mjd_start = h[0].header['MJD-OBS']
        mjd0 = 2400000.5
    else:
        #print(datetu)
        mjd0,mjd,i = GLOBALutils.iau_cal2jd(int(datetu[:4]),int(datetu[5:7]),int(datetu[8:10]))
        ho = int(datetu[11:13])
        mi = int(datetu[14:16])
        se = float(datetu[17:])
        ut = float(ho) + float(mi)/60.0 + float(se)/3600.0
        mjd_start = mjd + ut/24.0

    secinday = 24*3600.0
    fraction = 0.5
    texp     = h[0].header['EXPTIME'] #sec

    mjd = mjd_start + (fraction * texp) / secinday

    return mjd, mjd0

def Lines_mBack(thar, sd,  thres_rel=3, pl=False):
    """
    Given an extracted ThAr order, return a version where the background has been removed
    """


    L = np.where(sd > 0)

    # initial background estimate, simple 25% percentile on a 100-pixel filter
    bkg = np.zeros( len(sd) )
    bkg[L] = scipy.signal.order_filter(thar[L],np.ones(101),25)
    d = thar - bkg

    lines = FindLines_simple_sigma(d,sd, thres=thres_rel)
    # Now, mask these lines
    mask = np.ones( len(sd) )

    for kk in lines:
        #mask region
        if (d[kk] > 10000):
            mask[kk-3:kk+4] = 0
        elif (d[kk] > 5000):
            mask[kk-3:kk+4] = 0
        elif (d[kk] > 1000):
            mask[kk-3:kk+4] = 0
        else:
            mask[kk-3:kk+4] = 0

    # New, final background estimnate
    X = np.array( range( len( d ) ) )
    K = np.where((sd > 0) & (mask > 0))

    bkg = np.zeros( len(sd) )
    bkg_T = lowess(thar[K].astype('double'), X[K],frac=0.2,it=3,return_sorted=False)
    tck1 = scipy.interpolate.splrep(X[K],bkg_T,k=1)
    bkg[L] = scipy.interpolate.splev(X[L],tck1)

    return bkg

def FindLines_simple_sigma(d,sd,thres=3):
    """
    Given an array, find lines above a sigma-threshold
    """
    L = np.where( (d > (thres*sd)) & (d > 0) )
    lines = []
    for i in range(np.shape(L)[1]):
        j = L[0][i]
        if ((d[j] > d[j-1]) & (d[j-1] > d[j-2]) & (d[j] > d[j+1]) & (d[j+1] > d[j+2])):
            lines.append(j)

    return lines

### wavelength calibration routines ###
def sigma_clip(vec,lim=3.0):
    while True:
        med = np.median(vec)
        res = vec - med
        rms = np.sqrt(np.var(res))
        I   = np.where(np.absolute(res) < lim*rms)[0]
        if len(I) == len(vec):
            break
        else:
            vec = vec[I]
    return vec

def get_dark(time,dnames,dtimes):
    print(dnames)
    print(dtimes)
    print(time)
    if len(dnames) == 0:
        return 0.
    elif len(dnames) == 1:
        darko = pyfits.getdata(dnames[0])
        dark  = darko * float(time)/float(dtimes[0])
        print(np.median(darko),np.median(dark))
        return darko
    elif len(dnames) == 2:
        sc0 = pyfits.getdata(dnames[0]).astype('float')
        sc1 = pyfits.getdata(dnames[1]).astype('float')
        t0,t1 = float(dtimes[0]),float(dtimes[1])
        m = (sc1 - sc0) / (t1 - t0)
        n = sc1 - m*t1
        darko =  m*float(time) + n
        print(np.median(sc0),np.median(sc1))
        print(np.median(sc1*time/dtimes[1]),np.median(sc0*time/dtimes[0]))
        print(np.median(darko))
        return darko


# ---------------------------------------------------------------------------
# Wavelength-solution quality: ThAr grading, reference selection and the
# calibration assessment (ceres3 1.2).
#
# Everything below is FEROS-specific and self-contained. It grades each ThAr
# global wavelength solution on the number of lines that survived the fit and on
# the TRUE global RMS of those lines (m/s), picks the nightly reference among the
# healthy ones, builds the science header cards that record the outcome, and
# summarises a reduced calibration directory for downstream consumers
# (calib_quality.json, ExoAutomata).
# ---------------------------------------------------------------------------
import json
import pickle
import time

REF_MIN_LINES  = 1200   # fewer surviving lines than this: at best 'degraded'
REF_MAX_RMS_MS = 150    # true global RMS above this (m/s): 'bad'

WAVSOL_VERSION        = 2   # wavsolpars.pkl layout that carries a 'quality' record
SHIFTS_VERSION        = 2   # shifts.pkl layout that carries per-ThAr grades
CALIB_QUALITY_VERSION = 1   # schema of assess_calibration() / calib_quality.json
CALIB_QUALITY_FILE    = 'calib_quality.json'

DRIFT_MAX_ERR_MS = 5.0      # drift precision the science frames are held to

# FEROS geometry of the global solution, used only to re-evaluate a pre-1.2
# pickle whose stored residuals do not match its stored lines (see
# _kept_residuals_ms). New pickles record these values themselves.
_FEROS_O0    = 8
_FEROS_OO0   = 26
_FEROS_NPIX  = 4096
_FEROS_NX    = 5
_FEROS_NM    = 7
_SPEED_OF_LIGHT = 299792458.0


def _finite(v):
    try:
        return v is not None and bool(np.isfinite(float(v)))
    except (TypeError, ValueError):
        return False


def grade_wavsol(nlines_ob, nlines_co, rms_ob, rms_co, floor_hit_ob=False, floor_hit_co=False):
    """
    Grade a ThAr global wavelength solution (object + comparison fibre).

    good     : min(nlines) >= REF_MIN_LINES, max(rms) <= REF_MAX_RMS_MS, no cull-floor hit
    degraded : max(rms) <= REF_MAX_RMS_MS otherwise (too few lines, or culling
               was stopped by the floor with outliers left)
    bad      : anything else, including missing numbers
    """
    if not all(_finite(v) for v in (nlines_ob, nlines_co, rms_ob, rms_co)):
        return 'bad'
    nmin = min(int(nlines_ob), int(nlines_co))
    rmax = max(float(rms_ob), float(rms_co))
    if rmax > REF_MAX_RMS_MS:
        return 'bad'
    if nmin >= REF_MIN_LINES and not (floor_hit_ob or floor_hit_co):
        return 'good'
    return 'degraded'


def wavsol_quality_record(nlines_ob, nlines_co, rms_ob, rms_co, info_ob=None, info_co=None):
    """The 'quality' dict stored in a version-2 wavsolpars.pkl."""
    info_ob = info_ob or {}
    info_co = info_co or {}
    q = {'nlines_ob': int(nlines_ob), 'nlines_co': int(nlines_co),
         'rms_ob': float(rms_ob), 'rms_co': float(rms_co),
         'floor_hit_ob': bool(info_ob.get('floor_hit', False)),
         'floor_hit_co': bool(info_co.get('floor_hit', False)),
         'n_initial_ob': int(info_ob.get('n_initial', nlines_ob)),
         'n_initial_co': int(info_co.get('n_initial', nlines_co))}
    q['grade'] = grade_wavsol(q['nlines_ob'], q['nlines_co'], q['rms_ob'], q['rms_co'],
                              q['floor_hit_ob'], q['floor_hit_co'])
    return q


def _kept_residuals_ms(pdict, suf):
    """
    Residuals (m/s) of the lines a stored global fit kept, against its solution.

    G_res is in wavelength units. A pre-1.2 fit that stopped culling on crossing
    minlines returned residuals of the previous iteration (more entries than
    G_wav), so in that case they are re-evaluated from p1 on the kept lines.
    """
    wav = np.asarray(pdict['G_wav' + suf], dtype=float)
    res = np.asarray(pdict['G_res' + suf], dtype=float)
    if len(res) == len(wav):
        return _SPEED_OF_LIGHT * res / wav
    pix  = np.asarray(pdict['G_pix' + suf], dtype=float)
    ords = np.asarray(pdict['G_ord' + suf], dtype=float)
    o0   = int(pdict.get('o0', _FEROS_O0))
    oo0  = int(pdict.get('order0', _FEROS_OO0))
    npix = int(pdict.get('npix', _FEROS_NPIX))
    ntot = pdict.get('n_useful')
    if ntot is None:
        ntot = int(np.max(np.asarray(pdict['All_Orders' + suf]))) - o0 + 1
    chebs = GLOBALutils.Calculate_chebs(pix, ords + oo0, Inverse=True, order0=oo0, ntotal=int(ntot),
                                        npix=npix, nx=_FEROS_NX, nm=_FEROS_NM)
    model = (1.0 / (ords + oo0)) * GLOBALutils.Joint_Polynomial_Cheby(pdict['p1' + suf], chebs,
                                                                      nx=_FEROS_NX, nm=_FEROS_NM)
    return _SPEED_OF_LIGHT * (model - wav) / wav


def _legacy_fibre_quality(pdict, suf):
    """(nlines, rms_ms, floor_hit, n_initial) of one fibre of a pre-1.2 pickle."""
    n = len(pdict['II' + suf])
    r = _kept_residuals_ms(pdict, suf)
    rms = float(np.sqrt(np.var(r))) if len(r) else float('nan')
    # Pre-1.2 culling had minlines as its floor: it stopped (or never started)
    # below it, leaving outliers in. Outliers still present among the kept lines,
    # or residuals that do not match them, mean culling was cut short.
    floor_hit = (len(np.asarray(pdict['G_res' + suf])) != len(np.asarray(pdict['G_wav' + suf]))) \
        or bool(len(r) and np.any(np.absolute(r) > 4.0 * rms))
    n_initial = len(pdict.get('All_Wavelengths' + suf, pdict['II' + suf]))
    return n, rms, floor_hit, n_initial


def wavsol_quality(pdict):
    """
    Quality record of a wavsolpars dict of any version.

    Version-2 pickles carry it; for older ones it is derived from the stored
    global fits (the stored 'rms_ms' of those is not the global RMS and is
    ignored). Never raises: an unreadable record grades 'bad'.
    """
    q = pdict.get('quality') if isinstance(pdict, dict) else None
    if isinstance(q, dict) and (pdict.get('wavsol_version', 1) or 1) >= WAVSOL_VERSION:
        q = dict(q)
        q['grade'] = grade_wavsol(q.get('nlines_ob'), q.get('nlines_co'), q.get('rms_ob'),
                                  q.get('rms_co'), q.get('floor_hit_ob', False), q.get('floor_hit_co', False))
        return q
    try:
        n_ob, r_ob, fh_ob, ni_ob = _legacy_fibre_quality(pdict, '')
        n_co, r_co, fh_co, ni_co = _legacy_fibre_quality(pdict, '_co')
    except Exception:
        return {'nlines_ob': None, 'nlines_co': None, 'rms_ob': None, 'rms_co': None,
                'floor_hit_ob': None, 'floor_hit_co': None, 'n_initial_ob': None,
                'n_initial_co': None, 'grade': 'bad'}
    q = wavsol_quality_record(n_ob, n_co, r_ob, r_co,
                              {'floor_hit': fh_ob, 'n_initial': ni_ob},
                              {'floor_hit': fh_co, 'n_initial': ni_co})
    return q


def load_wavsol_quality(pkl_path):
    """wavsol_quality() of a pickle on disk; never raises."""
    try:
        with open(pkl_path, 'rb') as f:
            pdict = pickle.load(f, encoding='latin1')
    except Exception:
        pdict = None
    if not isinstance(pdict, dict):
        return wavsol_quality(None)
    return wavsol_quality(pdict)


def reference_tier(grades):
    """The best grade present among the candidate ThArs ('bad' if none is better)."""
    for tier in ('good', 'degraded'):
        if tier in grades:
            return tier
    return 'bad'


def select_reference(grades, difs=None, rms=None):
    """
    Pick the nightly reference ThAr.

    Only the best tier present is eligible: 'good' if any, else 'degraded'.
    Within it the pre-1.2 criterion is kept (smallest |mean object-minus-
    comparison shift| in ``difs``, first one on ties; the first candidate when no
    metric is available). If every ThAr is 'bad', the one with the smallest
    ``rms`` is used. Returns (index, grade).
    """
    grades = list(grades)
    if not grades:
        raise ValueError('no ThAr candidates to select a reference from')
    tier = reference_tier(grades)
    if tier != 'bad':
        idx = [i for i, g in enumerate(grades) if g == tier]
        if difs is not None:
            with_metric = [i for i in idx if _finite(difs[i])]
            if with_metric:
                return min(with_metric, key=lambda i: float(difs[i])), tier
        return idx[0], tier
    if rms is not None:
        with_rms = [i for i in range(len(grades)) if _finite(rms[i])]
        if with_rms:
            return min(with_rms, key=lambda i: float(rms[i])), 'bad'
    return 0, 'bad'


def _ceres3_version():
    try:
        from importlib.metadata import version
        return version('ceres3')
    except Exception:
        return 'unknown'


def _ref_summary(ref_quality):
    """(grade, min nlines, max rms) of a reference quality record."""
    q = ref_quality or {}
    grade = q.get('grade') or 'bad'
    nl = [q.get('nlines_ob'), q.get('nlines_co')]
    rm = [q.get('rms_ob'), q.get('rms_co')]
    nmin = min(int(v) for v in nl) if all(_finite(v) for v in nl) else None
    rmax = max(float(v) for v in rm) if all(_finite(v) for v in rm) else None
    return grade, nmin, rmax


def wavsol_flag_reason(drift_ok, ref_quality, drift_error_ms=None, maxlen=40):
    """Short, card-sized reason for the WAVSOL flag ('ok' when nothing is wrong)."""
    grade, nmin, rmax = _ref_summary(ref_quality)
    parts = []
    if grade == 'degraded':
        if nmin is not None and nmin < REF_MIN_LINES:
            parts.append(f'ref degraded {nmin} lines')
        else:
            parts.append('ref degraded cull floor hit')
    elif grade != 'good':
        parts.append(f'ref bad rms {rmax:.0f} m/s' if rmax is not None else 'ref bad no solution')
    if not drift_ok:
        if _finite(drift_error_ms):
            parts.append(f'drift {float(drift_error_ms):.1f} m/s > {DRIFT_MAX_ERR_MS:g}')
        else:
            parts.append('no drift anchor')
    reason = '; '.join(parts) if parts else 'ok'
    return reason[:maxlen]


def wavsol_quality_cards(drift_ok, ref_quality, drift_error_ms=None):
    """
    Header cards recording the wavelength-solution quality of a science frame.

    Returns [(keyword, value, comment), ...] for GLOBALutils.update_header.
    GOOD QUALITY WAVSOL is the AND of the drift measurement being good to
    DRIFT_MAX_ERR_MS and the nightly reference ThAr being graded 'good'.
    """
    grade, nmin, rmax = _ref_summary(ref_quality)
    drift_ok = bool(drift_ok)
    ref_ok = grade == 'good'
    return [
        ('HIERARCH GOOD QUALITY WAVSOL', bool(drift_ok and ref_ok), ''),
        ('HIERARCH GOOD QUALITY DRIFT', drift_ok, ''),
        ('HIERARCH GOOD QUALITY REFSOL', bool(ref_ok), ''),
        ('HIERARCH WAVSOL REF GRADE', str(grade), ''),
        ('HIERARCH WAVSOL REF NLINES', int(nmin) if nmin is not None else -1, ''),
        ('HIERARCH WAVSOL REF RMS', float(np.around(rmax, 1)) if rmax is not None else -999.0, '[m/s]'),
        ('HIERARCH WAVSOL FLAG REASON', wavsol_flag_reason(drift_ok, ref_quality, drift_error_ms), ''),
        ('HIERARCH CERES3 VERSION', _ceres3_version(), ''),
    ]


def _jsonable(v):
    """Recursively turn numpy containers/scalars into plain JSON types."""
    if isinstance(v, dict):
        return {str(k): _jsonable(x) for k, x in v.items()}
    if isinstance(v, (list, tuple, np.ndarray)):
        return [_jsonable(x) for x in v]
    if isinstance(v, np.bool_):
        return bool(v)
    if isinstance(v, np.integer):
        return int(v)
    if isinstance(v, (float, np.floating)):
        return float(v) if np.isfinite(v) else None
    return v


def _trace_summary(trace_dict, npix=_FEROS_NPIX):
    """(trace_ok, trace-info dict, reason or None) for a loaded trace.pkl dict."""
    info = {}
    try:
        if not isinstance(trace_dict, dict):
            raise TypeError('not a dict')
        c_all = np.asarray(trace_dict['c_all'])
        info['n_rows'] = int(c_all.shape[0])
        info['nord_ob'] = int(trace_dict.get('nord_ob', -1))
        info['nord_co'] = int(trace_dict.get('nord_co', -1))
    except Exception as e:
        return False, info, f'trace.pkl is unreadable ({type(e).__name__}); re-run the calibration'
    version = int(trace_dict.get('trace_version', 1) or 1)
    info['version'] = version
    if version >= 2:
        label = None
        for key in ('label_info', 'trace_info', 'labels'):
            if isinstance(trace_dict.get(key), dict):
                label = trace_dict[key]
                break
        if label is None:
            label = {k: trace_dict[k] for k in ('n_raw', 'offset_px', 'matched', 'synthesized',
                                                'max_residual_px') if k in trace_dict}
        info.update(_jsonable(label))
        info['check'] = 'labelled'
        return True, info, None
    if info['n_rows'] != 72:
        info['check'] = 'row-count'
        return False, info, (f"trace has {info['n_rows']} traces instead of 72 (pre-1.2 order "
                             "labelling lost traces, so orders are misregistered); re-run the "
                             "calibration with ceres3>=1.2")
    legacy_ok = globals().get('legacy_trace_ok')
    if callable(legacy_ok):
        info['check'] = 'legacy_trace_ok'
        try:
            ok = bool(legacy_ok(c_all, npix))
        except Exception:
            ok = False
        if not ok:
            return False, info, ('pre-1.2 trace is not the identity order labelling (orders are '
                                 'misregistered); re-run the calibration with ceres3>=1.2')
        return True, info, None
    # Without the template matcher only the row count can be checked.
    info['check'] = 'row-count'
    return True, info, None


def _thar_name(pkl_path):
    return os.path.basename(pkl_path)[:-len('wavsolpars.pkl')].rstrip('.')


def _load_pickle(path):
    with open(path, 'rb') as f:
        return pickle.load(f, encoding='latin1')


def describe_quality(q):
    """'<min lines> lines, rms <max rms> m/s (grade <grade>)' for logs and warnings."""
    grade, nmin, rmax = _ref_summary(q)
    r = f'{rmax:.0f}' if rmax is not None else 'n/a'
    n = nmin if nmin is not None else 'n/a'
    return f'{n} lines, rms {r} m/s (grade {grade})'


def _assess_products(calib_dir, thar_pkls=None, reference_pkl=None, trace_dict=None):
    """
    Assessment dict of a reduced calibration directory, from its products.

    thar_pkls     : the ThAr wavsolpars pickles to grade (default: all in the dir)
    reference_pkl : the reference the pipeline used; when None it is re-derived
                    with select_reference() from the grades and shifts.pkl
    trace_dict    : an already-loaded trace.pkl dict (default: read from the dir)
    """
    calib_dir = os.path.abspath(calib_dir)
    night = os.path.basename(calib_dir.rstrip('/'))
    if night.endswith('_red'):
        night = night[:-4]
    reasons = []
    out = {'version': CALIB_QUALITY_VERSION, 'source': 'legacy', 'calib_dir': calib_dir,
           'healthy': False, 'trace_ok': False, 'reference_ok': False, 'reference_grade': None,
           'reference_pkl': None, 'reasons': reasons,
           'thar': {'n_total': 0, 'n_good': 0, 'n_degraded': 0, 'n_bad': 0, 'frames': []},
           'trace': {}}
    if not os.path.isdir(calib_dir):
        reasons.append(f'calibration directory {calib_dir} does not exist; reduce the calibration night first')
        return out

    # -- trace ------------------------------------------------------------
    if trace_dict is None:
        tpath = os.path.join(calib_dir, 'trace.pkl')
        if not os.path.isfile(tpath):
            reasons.append(f'no trace.pkl in {night}; re-run the calibration')
        else:
            try:
                trace_dict = _load_pickle(tpath)
            except Exception as e:
                reasons.append(f'trace.pkl in {night} is unreadable ({type(e).__name__}); re-run the calibration')
    if trace_dict is not None:
        trace_ok, tinfo, treason = _trace_summary(trace_dict)
        out['trace_ok'] = bool(trace_ok)
        out['trace'] = tinfo
        if treason:
            reasons.append(treason)

    # -- ThAr solutions ---------------------------------------------------
    if thar_pkls is None:
        thar_pkls = sorted(glob.glob(os.path.join(calib_dir, '*wavsolpars.pkl')))
    thar_pkls = [os.path.abspath(p) for p in thar_pkls]
    quals, versions = [], []
    for p in thar_pkls:
        try:
            pdict = _load_pickle(p)
        except Exception:
            pdict = None
        pdict = pdict if isinstance(pdict, dict) else None
        versions.append(int(pdict.get('wavsol_version', 1) or 1) if pdict is not None else 0)
        quals.append(wavsol_quality(pdict))
    grades = [q['grade'] for q in quals]
    out['thar'] = {'n_total': len(quals), 'n_good': grades.count('good'),
                   'n_degraded': grades.count('degraded'), 'n_bad': grades.count('bad'),
                   'frames': [dict(name=_thar_name(p), **_jsonable(q)) for p, q in zip(thar_pkls, quals)]}
    if not quals and reference_pkl is None:
        reasons.append(f'no ThAr wavelength solutions (wavsolpars.pkl) in {night}; re-run the calibration')
        return _finish(out)

    # -- reference ----------------------------------------------------------
    shifts = None
    spath = os.path.join(calib_dir, 'shifts.pkl')
    if os.path.isfile(spath):
        try:
            shifts = _load_pickle(spath)
        except Exception:
            shifts = None
    names = [p[:-len('wavsolpars.pkl')] for p in thar_pkls]    # raw-file stem incl. trailing '.'
    stems = [os.path.basename(n) for n in names]

    def _vals_by_stem(dct):
        if not isinstance(dct, dict) or 'names' not in dct or 'vals' not in dct:
            return None
        m = {}
        for nm, v in zip(dct['names'], dct['vals']):
            m[os.path.basename(str(nm))[:-len('fits')]] = v
        vals = [m.get(s) for s in stems]
        return vals if any(_finite(v) for v in vals) else None

    rms = [max(q['rms_ob'], q['rms_co']) if _finite(q.get('rms_ob')) and _finite(q.get('rms_co'))
           else None for q in quals]
    legacy_shifts = shifts is not None and int(shifts.get('version', 1) or 1) < SHIFTS_VERSION
    legacy_pickles = any(v < WAVSOL_VERSION for v in versions)

    if reference_pkl is None and quals:
        ref_i = None
        if shifts is not None and not legacy_shifts and shifts.get('ref_name'):
            ref_stem = os.path.basename(str(shifts['ref_name']))[:-len('fits')]
            if ref_stem in stems:
                ref_i = stems.index(ref_stem)
        if ref_i is None:
            ref_i, _ = select_reference(grades, _vals_by_stem(shifts), rms)
        reference_pkl = thar_pkls[ref_i]
        ref_q = quals[ref_i]
    elif reference_pkl is not None:
        reference_pkl = os.path.abspath(reference_pkl)
        ref_q = load_wavsol_quality(reference_pkl)
    out['reference_pkl'] = reference_pkl
    out['reference_grade'] = ref_q['grade']
    out['reference'] = _jsonable(ref_q)
    out['reference_ok'] = ref_q['grade'] == 'good'
    if not out['reference_ok']:
        if os.path.dirname(reference_pkl) != calib_dir:     # an external -ref_thar
            reasons.append(f"reference ThAr {_thar_name(reference_pkl)} (-ref_thar) is not healthy: "
                           f"{describe_quality(ref_q)}; use a healthy night's reference")
        else:
            reasons.append(f"no healthy reference ThAr on {night}: best has {describe_quality(ref_q)}; "
                           "reduce its science against a healthy night's reference (-ref_thar)")

    # What a pre-1.2 ceres3 reduced this night's science against: the argmin of
    # the shifts.pkl metric over ALL ThArs, or the earliest one without it.
    if quals and (legacy_shifts or (shifts is None and legacy_pickles)):
        vals = _vals_by_stem(shifts) if shifts is not None else None
        if vals is not None:
            li = min((i for i in range(len(vals)) if _finite(vals[i])), key=lambda i: float(vals[i]))
        else:
            li = 0
        out['legacy_reference_pkl'] = thar_pkls[li]
        out['legacy_reference_grade'] = quals[li]['grade']
        if quals[li]['grade'] != 'good':
            reasons.append(f"pre-1.2 ceres3 reduced {night} against {stems[li].rstrip('.')}: "
                           f"{describe_quality(quals[li])}; re-reduce that science with ceres3>=1.2")
    return _finish(out)


def _finish(out):
    out['healthy'] = bool(out['trace_ok'] and out['reference_ok'])
    return out


def calib_quality_report(calib_dir, thar_pkls=None, reference_pkl=None, trace_dict=None, warnings=None):
    """The calib_quality.json payload an -is_calib run writes (source='pipeline')."""
    out = _assess_products(calib_dir, thar_pkls=thar_pkls, reference_pkl=reference_pkl,
                           trace_dict=trace_dict)
    out['source'] = 'pipeline'
    out['ceres3_version'] = _ceres3_version()
    out['created_utc'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
    out['warnings'] = list(warnings or [])
    return _jsonable(out)


def write_calib_quality(dirout, report):
    """Write calib_quality.json atomically into dirout; returns its path."""
    path = os.path.join(dirout, CALIB_QUALITY_FILE)
    tmp = path + '.tmp'
    with open(tmp, 'w') as f:
        json.dump(_jsonable(report), f, indent=1, sort_keys=True)
    os.replace(tmp, path)
    return path


def assess_calibration(calib_dir):
    """
    Assess a reduced FEROS calibration directory.

    Returns a dict with: version (int), source ('pipeline' when read from the
    calib_quality.json an -is_calib run of ceres3>=1.2 wrote, else 'legacy':
    derived from trace.pkl and the wavsolpars pickles), healthy, trace_ok,
    reference_ok, reference_grade ('good'|'degraded'|'bad'|None), reference_pkl
    (absolute path or None), reasons (actionable strings), thar {n_total, n_good,
    n_degraded, n_bad, frames}, trace {...}. Pre-1.2 directories additionally
    report legacy_reference_pkl / legacy_reference_grade: the reference the old
    selection picked. Never raises: a missing or corrupt directory comes back
    with healthy=False and a reason.
    """
    try:
        calib_dir = os.path.abspath(str(calib_dir))
        qpath = os.path.join(calib_dir, CALIB_QUALITY_FILE)
        json_problem = None
        if os.path.isfile(qpath):
            try:
                with open(qpath) as f:
                    rep = json.load(f)
                if not isinstance(rep, dict) or 'healthy' not in rep:
                    raise ValueError('not an assessment')
            except Exception as e:
                json_problem = f'{CALIB_QUALITY_FILE} is unreadable ({type(e).__name__}); assessed from the pickles'
            else:
                rep['source'] = 'pipeline'
                rep.setdefault('version', CALIB_QUALITY_VERSION)
                rep.setdefault('reasons', [])
                rep.setdefault('thar', {'n_total': 0, 'n_good': 0, 'n_degraded': 0, 'n_bad': 0})
                rep.setdefault('trace', {})
                for k in ('healthy', 'trace_ok', 'reference_ok'):
                    rep[k] = bool(rep.get(k, False))
                rep.setdefault('reference_grade', None)
                # The pipeline records container paths; follow the directory if it moved.
                ref = rep.get('reference_pkl')
                if ref and not os.path.isfile(ref):
                    local = os.path.join(calib_dir, os.path.basename(ref))
                    if os.path.isfile(local):
                        rep['reference_pkl'] = local
                rep['calib_dir'] = calib_dir
                return rep
        out = _assess_products(calib_dir)
        if json_problem:
            out['reasons'].append(json_problem)
        return out
    except Exception as e:  # never raise to the caller
        return {'version': CALIB_QUALITY_VERSION, 'source': 'legacy', 'calib_dir': str(calib_dir),
                'healthy': False, 'trace_ok': False, 'reference_ok': False, 'reference_grade': None,
                'reference_pkl': None,
                'reasons': [f'calibration could not be assessed ({type(e).__name__}: {e}); re-run it'],
                'thar': {'n_total': 0, 'n_good': 0, 'n_degraded': 0, 'n_bad': 0, 'frames': []},
                'trace': {}}
