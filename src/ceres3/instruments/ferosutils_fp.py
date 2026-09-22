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
# Order-trace labelling
#
# ferospipe_fp used to drop the first two traces returned by GLOBALutils.get_them
# (c_all[2:], meant to be the order -1 pair) and assume the rest alternate ob, co
# from order 0 upwards. When the master flat misses a trace -- typically the weak
# red-end comparison traces co-1/co0 -- every later order is then calibrated with
# its neighbour's line list, and an odd trace count crashes the ob/co split.
# label_traces identifies every detected trace instead, by matching the set to a
# template of the 74 physical traces (orders -1..35 x ob/co) with one global
# cross-dispersion offset.
# ---------------------------------------------------------------------------

TRACE_TEMPLATE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                   'data', 'feros_trace_template.txt')
TRACE_N_ORDERS      = 36              # orders 0..35 are kept, one ob and one co trace each
TRACE_O0            = 8               # ferospipe_fp o0: first wavelength-calibrated order
TRACE_N_USEFUL      = 25              # ferospipe_fp n_useful: number of calibrated orders
TRACE_OFFSET_RANGE  = (-10.0, 10.0)   # plausible global offset w.r.t. the template [px]; 2017-2026 span -6.5..+5.25
TRACE_SEARCH_MARGIN = 3.0             # the offset search extends this far beyond the plausible range [px]
TRACE_SEARCH_STEP   = 0.05            # [px]
TRACE_MATCH_CAP     = 2.0             # truncation of the robust matching cost [px]
TRACE_USED_TOL      = 2.0             # max residual of a trace in the used order range [px]; healthy nights <= 1.43
TRACE_OUTER_TOL     = 6.0             # max residual outside it [px]; weak edge traces are fitted up to ~4.8 px off
TRACE_AMBIG_MARGIN  = 0.25            # runner-up offset must leave >= 25% more of the set unexplained
TRACE_EVAL_FRACS    = (0.125, 0.5, 0.875)  # columns (fraction of npix) where residuals are measured

_trace_template_cache = {}


class FerosTraceError(RuntimeError):
    """The FEROS order traces cannot be labelled (or a calibration's traces are
    misregistered): reducing on would calibrate orders with the wrong line lists."""
    PREFIX = 'FEROS trace labelling failed'

    def __init__(self, msg):
        msg = str(msg)
        if not msg.startswith(self.PREFIX):
            msg = f'{self.PREFIX}: {msg}'
        super().__init__(msg)


def _trace_label(order, fibre):
    return f'{fibre}{int(order)}'


def load_trace_template(path=None):
    """Load the packaged FEROS trace template (see the data file header for how it
    was built). Returns a dict with 'order' (int array, -1..35), 'fibre' ('ob'/'co'),
    'labels', 'coeffs' (np.polyval order, highest power first), 'npix', 'meta', 'path'.
    Rows are sorted by increasing cross-dispersion position."""
    path = TRACE_TEMPLATE_FILE if path is None else path
    if path in _trace_template_cache:
        return _trace_template_cache[path]
    meta, orders, fibres, coeffs = {}, [], [], []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            if line.startswith('#'):
                key, sep, val = line[1:].partition(':')
                if sep and key.strip() and ' ' not in key.strip():
                    meta[key.strip()] = val.strip()
                continue
            tok = line.split()
            orders.append(int(tok[0]))
            fibres.append(tok[1])
            coeffs.append([float(v) for v in tok[5:]])
    tmpl = {
        'order': np.array(orders, dtype=int),
        'fibre': np.array(fibres),
        'labels': [_trace_label(o, fb) for o, fb in zip(orders, fibres)],
        'coeffs': np.array(coeffs, dtype=float),
        'npix': int(meta['npix']),
        'meta': meta,
        'path': path,
    }
    _trace_template_cache[path] = tmpl
    return tmpl


def _match_traces(c_raw, npix, o0, n_useful, template):
    """Core of label_traces/legacy_trace_ok: global offset + nearest-template
    assignment. Returns (offset, slot, res, diag) where slot[i] is the template row
    of raw trace i (-1 if rejected) and res[i] its max |residual| over the evaluation
    columns. Raises FerosTraceError for an implausible or ambiguous offset, or when a
    trace inside the used order range cannot be explained."""
    tmpl = load_trace_template(template)
    if int(npix) != tmpl['npix']:
        raise FerosTraceError(f"flat has {npix} dispersion columns, the trace template "
                              f"{os.path.basename(tmpl['path'])} was built for {tmpl['npix']}")
    c_raw = np.atleast_2d(np.asarray(c_raw, dtype=float))
    n_raw = c_raw.shape[0]
    cols = [int(fr * npix) for fr in TRACE_EVAL_FRACS]
    ic = cols.index(int(0.5 * npix))
    yr = np.array([[np.polyval(c, x) for x in cols] for c in c_raw]).reshape(n_raw, len(cols))
    yt = np.array([[np.polyval(c, x) for x in cols] for c in tmpl['coeffs']])
    good = np.all(np.isfinite(yr), axis=1)
    if good.sum() == 0:
        raise FerosTraceError(f'no usable traces among the {n_raw} detected')

    # Robust cost of one global offset: truncated quadratic distance of every raw
    # trace to its nearest template trace at the centre column.
    lo, hi = TRACE_OFFSET_RANGE
    grid = np.arange(lo - TRACE_SEARCH_MARGIN, hi + TRACE_SEARCH_MARGIN + 1e-9, TRACE_SEARCH_STEP)
    d = np.abs(yr[good, ic][None, :, None] - (yt[None, None, :, ic] + grid[:, None, None])).min(axis=2)
    cost = (np.minimum(d, TRACE_MATCH_CAP)**2).sum(axis=1) / (good.sum() * TRACE_MATCH_CAP**2)
    ib = int(np.argmin(cost))
    far = np.abs(grid - grid[ib]) > 2 * TRACE_MATCH_CAP
    i2 = int(np.argmin(np.where(far, cost, np.inf))) if far.any() else ib
    diag = {'cost_best': float(cost[ib]), 'runner_up_offset_px': float(grid[i2]),
            'cost_runner_up': float(cost[i2])}
    if not (lo <= grid[ib] <= hi):
        raise FerosTraceError(f'best global offset {grid[ib]:+.2f} px is outside the plausible '
                              f'range {lo:+.1f}..{hi:+.1f} px of the trace template')
    if cost[i2] - cost[ib] < TRACE_AMBIG_MARGIN:
        raise FerosTraceError(f'ambiguous global offset: {grid[ib]:+.2f} px (cost {cost[ib]:.2f}) vs '
                              f'{grid[i2]:+.2f} px (cost {cost[i2]:.2f}) from {n_raw} detected traces')

    # Refine: nearest-template assignment, offset = median residual of the traces
    # within the cost cap, iterate.
    def assign(off):
        slot = np.full(n_raw, -1)
        r = np.full((n_raw, len(cols)), np.inf)
        slot[good] = np.argmin(np.abs(yr[good, ic][:, None] - (yt[None, :, ic] + off)), axis=1)
        r[good] = yr[good] - yt[slot[good]] - off
        return slot, r

    off = float(grid[ib])
    for _ in range(5):
        slot, r = assign(off)
        close = np.abs(r[:, ic]) <= TRACE_MATCH_CAP
        new = off + float(np.median(r[close, ic])) if close.any() else off
        converged = abs(new - off) < 1e-3
        off = new
        if converged:
            break
    slot, r = assign(off)
    res = np.max(np.abs(r), axis=1)
    if not (lo <= off <= hi):
        raise FerosTraceError(f'global offset {off:+.2f} px is outside the plausible range '
                              f'{lo:+.1f}..{hi:+.1f} px of the trace template')

    orders = tmpl['order']
    in_used = (orders >= o0) & (orders < o0 + n_useful)

    def y0(i):
        return f'y={yr[i, ic]:.1f}'

    for i in range(n_raw):
        if slot[i] < 0:
            continue
        tol = TRACE_USED_TOL if in_used[slot[i]] else TRACE_OUTER_TOL
        if res[i] > tol:
            if in_used[slot[i]]:
                raise FerosTraceError(f'trace at {y0(i)} does not match the template: nearest is '
                                      f"{tmpl['labels'][slot[i]]} (used order range) with a "
                                      f'{res[i]:.1f} px residual > {tol:.1f} px')
            slot[i] = -1
    for k in np.unique(slot[slot >= 0]):
        dup = np.where(slot == k)[0]
        if len(dup) > 1:
            if in_used[k]:
                raise FerosTraceError(f"{len(dup)} detected traces ({', '.join(y0(i) for i in dup)}) "
                                      f"compete for {tmpl['labels'][k]} in the used order range")
            slot[dup[dup != dup[np.argmin(res[dup])]]] = -1
    diag['y_centre'] = yr[:, ic]
    return off, slot, res, diag


def label_traces(c_raw, npix, o0=TRACE_O0, n_useful=TRACE_N_USEFUL, template=None):
    """Label the traces found by GLOBALutils.get_them on a FEROS master flat.

    c_raw : (n_raw, ncoef) trace polynomials (np.polyval order, x = dispersion column,
            y = row of the transposed flat), any count and any subset of the 74 traces.
    npix  : number of dispersion columns of the transposed flat (Flat.T.shape[1]).

    Returns (c_all, info). c_all has exactly 72 rows, orders 0..35 interleaved ob, co
    (the layout ferospipe_fp always assumed after c_all[2:]); detected traces are
    identified by matching the whole set to the template with one global offset.
    Traces missing OUTSIDE the used orders [o0, o0+n_useful) are synthesised from the
    template shifted by that offset and listed in info['synthesized']; a trace missing
    inside it, an unexplained trace inside it, or an implausible/ambiguous offset raise
    FerosTraceError.

    info: n_raw, offset_px, matched (kept rows taken from detected traces), synthesized
    (labels), max_residual_px (over kept detected traces), max_residual_used_px,
    dropped (labels of detected traces outside orders 0..35), rejected_y_px (centre
    positions of detected traces matching no template trace), cost_best,
    runner_up_offset_px, cost_runner_up, template."""
    tmpl = load_trace_template(template)
    c_raw = np.atleast_2d(np.asarray(c_raw, dtype=float))
    off, slot, res, diag = _match_traces(c_raw, npix, o0, n_useful, template)
    ncoef = c_raw.shape[1]
    raw_of = {int(k): i for i, k in enumerate(slot) if k >= 0}
    c_all, synthesized, missing_used = [], [], []
    for order in range(TRACE_N_ORDERS):
        for fib in ('ob', 'co'):
            k = int(np.where((tmpl['order'] == order) & (tmpl['fibre'] == fib))[0][0])
            if k in raw_of:
                c_all.append(c_raw[raw_of[k]])
                continue
            if o0 <= order < o0 + n_useful:
                missing_used.append(_trace_label(order, fib))
                continue
            ct = tmpl['coeffs'][k].copy()
            ct[-1] += off
            if len(ct) < ncoef:
                ct = np.concatenate([np.zeros(ncoef - len(ct)), ct])
            elif len(ct) > ncoef:
                x = np.arange(npix, dtype=float)
                ct = np.polyfit(x, np.polyval(ct, x), ncoef - 1)
            c_all.append(ct)
            synthesized.append(_trace_label(order, fib))
    if missing_used:
        raise FerosTraceError(f"{len(missing_used)} trace(s) in the used order range "
                              f"{o0}..{o0 + n_useful - 1} not found on the master flat: "
                              f"{', '.join(missing_used)} ({c_raw.shape[0]} traces detected, "
                              f"global offset {off:+.2f} px)")
    c_all = np.array(c_all)
    yc = np.array([np.polyval(c, int(0.5 * npix)) for c in c_all])
    if np.any(np.diff(yc) <= 0):
        raise FerosTraceError('labelled traces are not ordered in cross-dispersion')
    kept = [i for i, k in enumerate(slot) if k >= 0 and 0 <= tmpl['order'][k] < TRACE_N_ORDERS]
    used = [i for i in kept if o0 <= tmpl['order'][slot[i]] < o0 + n_useful]
    info = {
        'n_raw': int(c_raw.shape[0]),
        'offset_px': float(off),
        'matched': len(kept),
        'synthesized': synthesized,
        'max_residual_px': float(max(res[kept])) if kept else float('nan'),
        'max_residual_used_px': float(max(res[used])) if used else float('nan'),
        'dropped': [tmpl['labels'][slot[i]] for i in range(len(slot))
                    if slot[i] >= 0 and not 0 <= tmpl['order'][slot[i]] < TRACE_N_ORDERS],
        'rejected_y_px': [round(float(diag['y_centre'][i]), 2) for i in range(len(slot)) if slot[i] < 0],
        'cost_best': diag['cost_best'],
        'runner_up_offset_px': diag['runner_up_offset_px'],
        'cost_runner_up': diag['cost_runner_up'],
        'template': os.path.basename(tmpl['path']),
    }
    return c_all, info


def legacy_trace_ok(c_all, npix, o0=TRACE_O0, n_useful=TRACE_N_USEFUL, template=None):
    """True iff a pre-1.2 72-row c_all (get_them output after c_all[2:]) is the
    identity labelling, i.e. row 2j is order j ob and row 2j+1 order j co for every
    row. Never raises."""
    try:
        c_all = np.asarray(c_all, dtype=float)
        if c_all.ndim != 2 or c_all.shape[0] != 2 * TRACE_N_ORDERS:
            return False
        tmpl = load_trace_template(template)
        off, slot, res, diag = _match_traces(c_all, npix, o0, n_useful, template)
        want = [int(np.where((tmpl['order'] == j // 2) &
                             (tmpl['fibre'] == ('ob' if j % 2 == 0 else 'co')))[0][0])
                for j in range(2 * TRACE_N_ORDERS)]
        return bool(np.array_equal(slot, want))
    except Exception:
        return False
