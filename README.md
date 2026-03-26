# CERES3

A Python 3 port of [CERES](https://github.com/rabrahm/ceres) — a collection of fully automated pipelines for the reduction, extraction and analysis of echelle spectrograph data. CERES3 produces wavelength-calibrated spectra, precise radial velocities, bisector spans, and chromospheric activity indicators (S-index, H-alpha, HeI, NaI D1/D2).

If you use this code, please cite [Brahm et al 2017](https://ui.adsabs.harvard.edu/abs/2017PASP..129c4002B).

## Installation

### System prerequisites

```bash
# Debian/Ubuntu
sudo apt install gfortran libgsl-dev libopenblas-dev build-essential pkg-config

# macOS (Homebrew)
brew install gcc gsl openblas
```

### Install CERES3

```bash
pip install ceres3
```

Or from source:
```bash
git clone https://github.com/jvines/ceres3.git
cd ceres3
pip install .
```

This compiles the C/Fortran extensions (Marsh optimal extraction, CCF cross-correlation) automatically.

## Quick start

```bash
# Step 1: Reduce calibration frames (bias, flat, ThAr)
ceres3 feros /path/to/raw/calibrations --is-calib

# Step 2: Reduce science frames
ceres3 feros /path/to/raw/science --calib-dir /path/to/calibrations_red/
```

Output goes to `{input_dir}_red/` by default. Override with `-o /custom/path`.

## Supported instruments

| Instrument | Telescope | Pipeline |
|------------|-----------|----------|
| ARCES | APO 3.5m | `ceres3 arces` |
| CAFE | CAHA 2.2m | `ceres3 cafe` |
| Coralie | Euler 1.2m | `ceres3 coralie` |
| Echelle | DuPont 2.5m | `ceres3 dupont` |
| ESPaDOnS | CFHT 3.6m | `ceres3 espadons` |
| FEROS | MPG/ESO 2.2m | `ceres3 feros` |
| FIDEOS | ESO 1.0m | `ceres3 fideos` |
| FIES | NOT 2.5m | `ceres3 fies` |
| HARPS | ESO 3.6m | `ceres3 harps` |
| HIRES | Keck 10m | `ceres3 hires` |
| MIKE | Magellan 6.5m | `ceres3 mike` |
| PFS | Magellan 6.5m | `ceres3 pfs` |
| PUCHEROS | PUC 0.5m | `ceres3 pucheros` |
| VBT Echelle | VBT 2.3m | `ceres3 vbt` |

## CLI options

```
ceres3 <instrument> <input_dir> [options]

Options:
  -o, --output-dir DIR     Output directory (default: {input_dir}_red/)
  --npools N               CPU cores to use (default: 4)
  --nsigmas N              Order detection sigma threshold (default: 5.0)
  --is-calib               Process as calibration data
  --calib-dir DIR          Path to pre-computed calibration directory
  --do-class               Enable spectral classification (Teff, logg, [Fe/H])
  --target NAME            Process only this target (default: all)
```

## Python API

```python
# Import and run a pipeline directly
from ceres3.instruments import get_pipeline_module
feros = get_pipeline_module('feros')
# Pipeline runs on import (module-level code)
```

## Data products

All final products are in the `proc/` subdirectory of the output directory.

### FITS spectrum cube (`*_sp.fits`)

Shape: `(11, n_orders, n_pixels)` — 11 data layers per echelle order:

| Layer | Contents |
|-------|----------|
| 0 | Wavelength (Angstroms) |
| 1 | Extracted flux |
| 2 | Flux error (1/sqrt(variance)) |
| 3 | Blaze-corrected flux |
| 4 | Blaze-corrected flux error |
| 5 | Continuum-normalized flux |
| 6 | Continuum-normalized flux error |
| 7 | Estimated continuum |
| 8 | Signal-to-noise ratio per pixel |
| 9 | Continuum-normalized flux × d(wavelength)/d(pixel) |
| 10 | Error of layer 9 |

### Merged 1D spectrum (`*_1d.fits`)

A single FITS binary table with columns `WAVELENGTH`, `FLUX`, `ERROR` — the echelle orders merged into one continuous spectrum using S/N-weighted crossover. This is the rest-frame, continuum-normalized spectrum suitable for input to spectral analysis tools (e.g., SPECIES).

### FITS header keywords

| Keyword | Description |
|---------|-------------|
| `RV` | Radial velocity (km/s) |
| `RV_E` | RV error (km/s) |
| `BS` | Bisector span (km/s) |
| `BS_E` | BS error (km/s) |
| `FWHM` | CCF full width at half maximum (km/s) |
| `XC_MIN` | CCF contrast (depth of minimum) |
| `BJD_OUT` | Barycentric Julian Date |
| `SNR` | Signal-to-noise at ~5130 A |
| `S_INDEX` | Ca H+K S-index |
| `HALPHA` | H-alpha activity index |
| `HEI` | HeI 5876 activity index |
| `NAI_D1D2` | NaI D1+D2 activity index |
| `SPEC1D` | Path to merged 1D spectrum |
| `INST` | Instrument name |
| `PIPELINE` | `CERES` |

### results.txt

Tab-separated summary with columns: object name, BJD, RV, RV error, bisector span, BS error, FWHM, instrument, pipeline, resolving power, Teff, log(g), [Fe/H], v*sin(i), XC_min, CCF dispersion, exposure time, SNR, S-index, H-alpha, HeI, NaI D1/D2, CCF plot path.

### CCF plots (`*.pdf`)

PDF files showing the cross-correlation function and Gaussian fit for each target.

## Auxiliary files

Place these in the raw data directory to customize the reduction:

### reffile.txt

Comma-separated file with target-specific parameters:

```
HD157347,17:22:51.28809,-02:23:17.4297,49.39,-107.16,1,G2,4.0
HD32147,05:00:48.99977,-05:45:13.2303,550.12,-1109.23,1,G2,4.0
```

Columns: name, RA (J2000), Dec (J2000), PM_RA (mas/yr), PM_Dec (mas/yr), use_coords (0/1), CCF mask (G2/K5/M2), velocity width (km/s).

### bad_files.txt

List filenames (one per line) to exclude from processing.

### moon_corr.txt

List filenames for which the CCF fit includes a double Gaussian to correct for scattered moonlight.

## Spectral classification

To estimate atmospheric parameters (Teff, log(g), [Fe/H], v*sin(i)), use `--do-class`. This requires the Coelho et al. (2005) model grid:

```bash
mkdir -p ~/.ceres3/COELHO_MODELS
cd ~/.ceres3/COELHO_MODELS
wget http://www.astro.puc.cl/~rbrahm/coelho_05_red4_R40.tar.gz
tar -xf coelho_05_red4_R40.tar.gz
```

## What changed from the original CERES

- **Python 3.10+** (was Python 2.7)
- **No R dependency** (rpy2 was never used)
- **No SWIG / SOFA / manual ephemeris updates** — astropy handles barycentric corrections with DE440 ephemeris (auto-downloaded)
- **Inline activity indicators** — S-index, H-alpha, HeI, NaI D1/D2 computed during reduction (no separate post-processing step)
- **Merged 1D spectrum output** — rest-frame continuum-normalized spectrum as a FITS binary table
- **Batch CCF** — Fortran subroutine processes all velocity steps in one call (3-4x faster)
- **pip installable** — `pip install ceres3` compiles extensions automatically

## Authors

Rafael Brahm, Andres Jordan, Nestor Espinoza, Jose Vines.

## License

MIT
