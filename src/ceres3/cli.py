"""Command-line interface for CERES3 spectral reduction."""

import argparse
import sys


def cmd_reduce(args):
    """Run a full instrument reduction pipeline."""
    from ceres3.instruments import get_pipeline_module
    pipeline = get_pipeline_module(args.instrument)

    # Reconstruct argv for backward compat with module-level argparse
    sys.argv = [f'ceres3-{args.instrument}', args.input_dir]
    if args.dirout:
        sys.argv.extend(['-dirout', args.dirout])
    sys.argv.extend(['-npools', str(args.npools)])
    sys.argv.extend(['-nsigmas', str(args.nsigmas)])
    if args.is_calib:
        sys.argv.append('-is_calib')
    if args.calib_dir:
        sys.argv.extend(['-calib_dir', args.calib_dir])
    if args.do_class:
        sys.argv.append('-do_class')
    if args.o2do != 'all':
        sys.argv.extend(['-o2do', args.o2do])

    import importlib
    importlib.reload(pipeline)


def cmd_activity(args):
    """Compute activity indicators on already-reduced spectra."""
    import os
    import glob
    import numpy as np
    from astropy.io import fits as pyfits
    from ceres3.utils.activity import compute_activity

    input_dir = args.input_dir.rstrip('/')
    proc_dir = os.path.join(input_dir, 'proc') if os.path.isdir(os.path.join(input_dir, 'proc')) else input_dir

    # Find all reduced spectrum cubes
    patterns = [os.path.join(proc_dir, '*_sp.fits')]
    fits_files = sorted(set(f for p in patterns for f in glob.glob(p)))

    if not fits_files:
        print(f"No *_sp.fits files found in {proc_dir}")
        sys.exit(1)

    print(f"Found {len(fits_files)} spectra in {proc_dir}")
    print(f"{'File':<60} {'S':>7} {'logRHK':>7} {'Ha':>7} {'Hb':>7} {'HeI':>7} {'NaI':>7} {'CaIRT':>7} {'MgIb':>7}")
    print("-" * 130)

    results = []
    for fpath in fits_files:
        fname = os.path.basename(fpath)
        try:
            hdul = pyfits.open(fpath)
            spec = hdul[0].data
            header = hdul[0].header

            # Get Teff if available
            teff = header.get('TEFF', None)
            if teff is not None and teff <= 0:
                teff = None

            instrument = header.get('INST', args.instrument or 'feros').lower()

            # Output 1D path
            out_1d = fpath.replace('_sp.fits', '_1d.fits') if args.save_1d else None

            activity = compute_activity(spec, instrument=instrument,
                                        output_1d_path=out_1d, teff=teff)

            # Write activity values back to FITS header
            if args.update_fits:
                with pyfits.open(fpath, mode='update') as hdu_update:
                    for key, val in activity.items():
                        if key == 'spectrum_1d_path':
                            continue
                        try:
                            hdu_update[0].header[key.upper()] = np.around(val, 6)
                        except Exception:
                            pass
                    hdu_update.flush()

            a = activity
            print(f"{fname:<60} {a['s_index']:>7.4f} {a['log_rhk']:>7.3f} {a['halpha']:>7.4f} {a['hbeta']:>7.4f} {a['hei']:>7.4f} {a['nai_d1d2']:>7.4f} {a['ca_irt']:>7.4f} {a['mg_ib']:>7.4f}")
            results.append({'file': fname, **activity})
            hdul.close()

        except Exception as e:
            print(f"{fname:<60} ERROR: {e}")

    # Write summary file
    if args.output:
        outpath = args.output
    else:
        outpath = os.path.join(proc_dir, 'activity.txt')

    with open(outpath, 'w') as f:
        header_line = "# file bjd s_index s_index_err log_rhk halpha halpha_err hbeta hbeta_err hei hei_err nai_d1d2 nai_d1d2_err ca_irt ca_irt_err mg_ib mg_ib_err\n"
        f.write(header_line)
        for r in results:
            bjd = r.get('bjd', 0.0)
            f.write(f"{r['file']} {r['s_index']:.6f} {r['s_index_err']:.6f} {r['log_rhk']:.4f} "
                    f"{r['halpha']:.6f} {r['halpha_err']:.6f} "
                    f"{r['hbeta']:.6f} {r['hbeta_err']:.6f} "
                    f"{r['hei']:.6f} {r['hei_err']:.6f} "
                    f"{r['nai_d1d2']:.6f} {r['nai_d1d2_err']:.6f} "
                    f"{r['ca_irt']:.6f} {r['ca_irt_err']:.6f} "
                    f"{r['mg_ib']:.6f} {r['mg_ib_err']:.6f}\n")

    print(f"\nResults written to {outpath}")
    if args.save_1d:
        n_1d = sum(1 for r in results if r.get('spectrum_1d_path'))
        print(f"1D spectra saved: {n_1d}/{len(results)}")


def main():
    from ceres3.instruments import list_instruments

    parser = argparse.ArgumentParser(
        prog='ceres3',
        description='CERES3: Echelle spectrograph data reduction pipeline',
    )
    subparsers = parser.add_subparsers(dest='command', help='Command to run')

    # ── reduce ────────────────────────────────────────────────────────
    p_reduce = subparsers.add_parser('reduce', help='Run instrument reduction pipeline')
    p_reduce.add_argument('instrument', choices=list_instruments())
    p_reduce.add_argument('input_dir')
    p_reduce.add_argument('-o', '--output-dir', default=None, dest='dirout')
    p_reduce.add_argument('--npools', type=int, default=4)
    p_reduce.add_argument('--nsigmas', type=float, default=5.0)
    p_reduce.add_argument('--is-calib', action='store_true')
    p_reduce.add_argument('--calib-dir', default=None)
    p_reduce.add_argument('--do-class', action='store_true')
    p_reduce.add_argument('--target', default='all', dest='o2do')

    # ── activity ──────────────────────────────────────────────────────
    p_activity = subparsers.add_parser('activity',
        help='Compute activity indicators on already-reduced spectra')
    p_activity.add_argument('input_dir',
        help='Directory containing reduced *_sp.fits files (or parent with proc/ subdir)')
    p_activity.add_argument('--instrument', default=None,
        help='Override instrument name (default: read from FITS header)')
    p_activity.add_argument('-o', '--output', default=None,
        help='Output file for activity summary (default: proc/activity.txt)')
    p_activity.add_argument('--save-1d', action='store_true',
        help='Save merged 1D rest-frame spectra as *_1d.fits')
    p_activity.add_argument('--update-fits', action='store_true',
        help='Write activity values back into the FITS headers')

    args = parser.parse_args()

    if args.command == 'reduce':
        cmd_reduce(args)
    elif args.command == 'activity':
        cmd_activity(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == '__main__':
    main()
