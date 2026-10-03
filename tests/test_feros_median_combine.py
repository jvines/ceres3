"""The FEROS master frames are combined without the int64 dstack (EXOAUTOMAT-266).

``ferosutils.MedianCombine`` grew its stack with a looped ``np.dstack`` of
``astype('int')`` frames: int64 on Linux, four times the raw BITPIX=16 width, and
re-copied on every frame so that the last append held two stacks at once. A
69-frame FEROS calibration set measured 8.9 GB peak per ceres job on predator.

It now fills one pre-allocated int32 cube and takes the median in place. That is
only acceptable if the master frames are unchanged, which is what most of this
file checks against a verbatim copy of the old function.
"""
from __future__ import annotations

import tracemalloc

import numpy as np
import pytest
from astropy.io import fits

from ceres3.instruments import ferosutils as F

# Raw FEROS frames are 4096 x 2148 and trim to 4096 x 2048. b_col marks bad
# columns up to x=1299, so a frame narrower than that would skip the repair;
# fewer rows keeps the suite fast without leaving any code path unexercised.
NY, NX_RAW = 96, 1500


def _legacy_median_combine(ImgList, zero_bo=False, zero='MasterBias.fits'):
    """ferosutils.MedianCombine as it was in ceres3 1.2.4, unchanged."""
    if zero_bo:
        BIAS = fits.getdata(zero)
    n = len(ImgList)
    if n == 0:
        raise ValueError("empty list provided!")
    h = fits.open(ImgList[0])[0]
    d = h.data
    d = F.OverscanTrim(d)
    d = F.b_col(d)
    if zero_bo:
        d -= BIAS
    d = np.round(d).astype('int')
    ronoise, gain = F.get_RG(h.header)
    ronoise = ronoise / np.sqrt(n)
    if n == 1:
        return d, ronoise, gain
    for i in range(n - 1):
        h = fits.open(ImgList[i + 1])[0]
        ot = F.OverscanTrim(h.data)
        ot = F.b_col(ot)
        if zero_bo:
            d = np.dstack((d, np.round((ot - BIAS)).astype('int')))
        else:
            d = np.dstack((d, np.round(ot).astype('int')))
    return np.median(d, axis=2), ronoise, gain


def _write_raw(path, level, rng, ny=NY, nx=NX_RAW, mode='normal'):
    """A raw-like frame: unsigned 16-bit counts on a sloped pedestal.

    The pre/overscan strips (48 columns each side) carry only the pedestal and
    read noise, as on the detector; the signal is in the image area, so a bright
    flat stays bright after overscan correction. Stored as FEROS stores it
    (BITPIX=16, BZERO=32768), so astropy hands the pipeline uint16 as on real data.
    """
    rows = np.arange(ny)[:, None]
    counts = 200.0 + 0.01 * rows + rng.normal(0.0, 3.0, (ny, nx))
    counts[:, 50:-50] += rng.poisson(level, (ny, nx - 100))
    hdu = fits.PrimaryHDU(np.clip(np.round(counts), 0, 65535).astype(np.uint16))
    hdu.header['HIERARCH ESO DET READ MODE'] = mode
    hdu.writeto(path)
    return str(path)


def _frames(tmp_path, n, level, seed, prefix):
    rng = np.random.default_rng(seed)
    return [_write_raw(tmp_path / f"{prefix}{i}.fits", level, rng) for i in range(n)]


def _master_bias(tmp_path, n=5):
    frames = _frames(tmp_path, n, 30, 11, "bias")
    bias, _, _ = F.MedianCombine(frames)
    path = tmp_path / "MasterBias.fits"
    fits.PrimaryHDU(bias).writeto(path)
    return str(path)


class TestSameMasterFrames:
    """Bit-identical to the old implementation, not merely close."""

    @pytest.mark.parametrize("n", [2, 3, 4, 7])
    def test_bias_combine(self, tmp_path, n):
        frames = _frames(tmp_path, n, 30, n, "b")
        new = F.MedianCombine(frames)
        old = _legacy_median_combine(frames)
        assert np.array_equal(new[0], old[0])
        assert new[0].dtype == old[0].dtype == np.float64
        assert new[1:] == old[1:]

    @pytest.mark.parametrize("n", [2, 5, 8])
    def test_flat_combine_with_bias_subtraction(self, tmp_path, n):
        zero = _master_bias(tmp_path)
        frames = _frames(tmp_path, n, 50000, 100 + n, "f")
        new = F.MedianCombine(frames, zero_bo=True, zero=zero)
        old = _legacy_median_combine(frames, zero_bo=True, zero=zero)
        # a bright flat is what rules out int16 storage
        assert new[0].max() > np.iinfo(np.int16).max
        assert np.array_equal(new[0], old[0])
        assert new[1:] == old[1:]

    def test_even_counts_average_the_two_middle_values(self, tmp_path):
        """An even stack yields half-integers; the float64 result must keep them."""
        frames = _frames(tmp_path, 4, 30, 7, "e")
        new, _, _ = F.MedianCombine(frames)
        assert np.any(new != np.round(new)), "no half-integer medians in the fixture"
        assert np.array_equal(new, _legacy_median_combine(frames)[0])

    def test_negative_bias_subtracted_values_survive(self, tmp_path):
        """Below-bias pixels go negative; uint16 or int16 storage would break them."""
        zero = _master_bias(tmp_path)
        frames = _frames(tmp_path, 3, 0, 5, "d")
        new, _, _ = F.MedianCombine(frames, zero_bo=True, zero=zero)
        assert new.min() < 0
        assert np.array_equal(new, _legacy_median_combine(frames, zero_bo=True, zero=zero)[0])

    def test_a_single_frame_is_returned_as_before(self, tmp_path):
        """No median, no cube: same int64 array, so the written FITS keeps BITPIX=64."""
        frames = _frames(tmp_path, 1, 30, 3, "s")
        new = F.MedianCombine(frames)
        old = _legacy_median_combine(frames)
        assert new[0].dtype == old[0].dtype
        assert np.array_equal(new[0], old[0])
        assert new[1:] == old[1:]

    @pytest.mark.parametrize("mode, ron, gain", [("normal", 5.1, 3.2), ("slow", 3.0, 1.0)])
    def test_read_noise_and_gain_come_from_the_first_frame(self, tmp_path, mode, ron, gain):
        rng = np.random.default_rng(0)
        frames = [_write_raw(tmp_path / f"m{i}.fits", 30, rng, mode=mode) for i in range(4)]
        _, ronoise, g = F.MedianCombine(frames)
        assert g == gain
        assert ronoise == pytest.approx(ron / 2.0)


class TestGuards:

    def test_an_empty_list_is_refused(self):
        with pytest.raises(ValueError, match="empty list"):
            F.MedianCombine([])

    def test_a_value_int32_cannot_hold_is_refused_not_wrapped(self):
        cube = np.empty((1, 2, 2), dtype=np.int32)
        frame = np.array([[0.0, 1.0], [2.0, 3.0e9]])
        with pytest.raises(OverflowError, match="int32"):
            F._store_rounded(cube, 0, frame)

    def test_the_int32_bounds_themselves_are_accepted(self):
        cube = np.empty((1, 1, 2), dtype=np.int32)
        info = np.iinfo(np.int32)
        F._store_rounded(cube, 0, np.array([[float(info.min), float(info.max)]]))
        assert cube[0, 0, 0] == info.min and cube[0, 0, 1] == info.max


class TestMemory:
    """The point of the change, measured with tracemalloc, which numpy reports to."""

    N = 24

    @staticmethod
    def _peak(fn, *args, **kwargs):
        tracemalloc.start()
        try:
            fn(*args, **kwargs)
            return tracemalloc.get_traced_memory()[1]
        finally:
            tracemalloc.stop()

    def test_peak_stays_below_a_single_int64_stack(self, tmp_path):
        frames = _frames(tmp_path, self.N, 30, 21, "p")
        ny, nx = NY, NX_RAW - 100
        int64_stack = self.N * ny * nx * 8
        peak = self._peak(F.MedianCombine, frames)
        assert peak < int64_stack, f"peak {peak/1e6:.1f} MB >= int64 stack {int64_stack/1e6:.1f} MB"

    def test_peak_is_well_under_the_old_implementation(self, tmp_path):
        frames = _frames(tmp_path, self.N, 30, 22, "q")
        new = self._peak(F.MedianCombine, frames)
        old = self._peak(_legacy_median_combine, frames)
        assert new < 0.5 * old, f"new {new/1e6:.1f} MB vs old {old/1e6:.1f} MB"
