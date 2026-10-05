"""Per-target astrometry and CCF mask for FEROS reductions (EXOAUTOMAT-297).

CERES read these from reffile.txt, which ExoAutomata never wrote: every star got its
header pointing with no proper motion (tau Ceti's BERV then carries an annual error of
K ~ 9 m/s) and the G2 mask (HD72673 moves by +28.7 m/s with K5). The targets file
carries an epoch-stamped ICRS position with full space motion and the mask.
"""
from __future__ import annotations

import ast
import json
from pathlib import Path

import numpy as np
import pytest

from ceres3 import targets as T

PIPE = (Path(__file__).resolve().parents[1]
        / "src" / "ceres3" / "instruments" / "ferospipe_fp.py")

# tau Ceti, Hipparcos (van Leeuwen 2007) astrometry at J2000; pmra is mu_alpha*.
TAU_CET = {"ra": 26.01701458, "dec": -15.93748, "epoch": 2000.0, "pmra": -1721.05,
           "pmdec": 854.16, "parallax": 273.96, "rv": -16.68, "mask": "G2"}


def doc(**targets):
    return {"version": 1, "targets": targets}


class TestParsing:
    def test_a_full_target(self):
        t = T.parse_targets(doc(**{"tau Cet": TAU_CET}))["taucet"]
        assert t.name == "tau Cet" and t.has_position and t.mask == "G2"
        assert t.pmra == -1721.05 and t.epoch == 2000.0

    def test_mask_only_target_keeps_the_header_position(self):
        t = T.parse_targets(doc(HD72673={"mask": "k5"}))["hd72673"]
        assert t.mask == "K5" and not t.has_position

    @pytest.mark.parametrize("entry, message", [
        ({"mask": "F0"}, "mask"),
        ({"ra": 10.0}, "together"),
        ({"ra": 10.0, "dec": 5.0}, "epoch"),
        ({"ra": 400.0, "dec": 5.0, "epoch": 2000.0}, "outside"),
        ({"ra": 10.0, "dec": -95.0, "epoch": 2000.0}, "outside"),
        ({"ra": "x", "dec": 5.0, "epoch": 2000.0}, "number"),
        ({"ra": 10.0, "dec": 5.0, "epoch": 2000.0, "pmra": float("nan")}, "finite"),
        ({"ccf_width": -1.0}, "positive"),
    ])
    def test_malformed_targets_are_refused(self, entry, message):
        with pytest.raises(T.TargetsError, match=message):
            T.parse_targets(doc(X=entry))

    @pytest.mark.parametrize("bad", [{}, {"version": 2, "targets": {}}, {"version": 1}, [1, 2]])
    def test_the_document_shape_is_checked(self, bad):
        with pytest.raises(T.TargetsError):
            T.parse_targets(bad)

    def test_names_that_collide_after_normalisation_are_refused(self):
        with pytest.raises(T.TargetsError, match="collide"):
            T.parse_targets(doc(**{"tau Cet": {"mask": "G2"}, "TAU_CET": {"mask": "K5"}}))

    def test_two_spellings_of_one_star_with_the_same_entry_are_fine(self):
        t = T.parse_targets(doc(**{"tau Cet": TAU_CET, "tauCet": TAU_CET}))
        assert len(t) == 1 and t["taucet"].mask == "G2"

    def test_a_non_positive_parallax_is_ignored(self):
        t = T.parse_targets(doc(X={"ra": 1.0, "dec": 1.0, "epoch": 2016.0, "parallax": -0.3}))["x"]
        assert t.parallax is None

    def test_no_file_means_no_targets(self):
        assert T.load_targets(None) == {}

    def test_load_from_disk(self, tmp_path):
        p = tmp_path / "targets.json"
        p.write_text(json.dumps(doc(**{"tau Cet": TAU_CET})))
        assert T.lookup(T.load_targets(str(p)), "tauCet").mask == "G2"


class TestLookup:
    @pytest.mark.parametrize("header_name", ["tau Cet", "tauCet", "TAU_CET", "tau-cet", " Tau  Cet "])
    def test_header_spellings_find_the_target(self, header_name):
        targets = T.parse_targets(doc(**{"tau Cet": TAU_CET}))
        assert T.lookup(targets, header_name) is not None

    def test_unknown_target(self):
        assert T.lookup(T.parse_targets(doc(**{"tau Cet": TAU_CET})), "HD10700") is None


class TestPropagation:
    def test_proper_motion_moves_the_position(self):
        t = T.parse_target("tau Cet", TAU_CET)
        mjd = 60247.65                       # 2023-10-30, 23.83 yr after J2000
        ra, dec = T.position_at(t, mjd)
        years = (mjd - 51544.5) / 365.25
        # first order: dec moves by mu_delta*t, RA by mu_alpha*/cos(dec)*t
        exp_dec = TAU_CET["dec"] + TAU_CET["pmdec"] / 3.6e6 * years
        exp_ra = TAU_CET["ra"] + TAU_CET["pmra"] / 3.6e6 * years / np.cos(np.radians(TAU_CET["dec"]))
        assert abs(dec - exp_dec) * 3600 < 0.05
        assert abs((ra - exp_ra) * np.cos(np.radians(dec))) * 3600 < 0.05
        # and it moved a lot: ~45 arcsec
        assert np.hypot((ra - TAU_CET["ra"]) * np.cos(np.radians(dec)), dec - TAU_CET["dec"]) * 3600 > 40

    def test_no_position_is_an_error(self):
        with pytest.raises(ValueError):
            T.position_at(T.parse_target("X", {"mask": "G2"}), 60000.0)

    def test_the_barycentric_correction_matches_astropy_with_proper_motion(self):
        """The pipeline's BERV path, fed the propagated position, reproduces astropy's
        correction for the moving star; a fixed J2000 position does not."""
        import astropy.units as u
        from astropy.coordinates import EarthLocation, SkyCoord
        from astropy.time import Time
        from ceres3.utils import globalutils as G, jplephem

        lat, lon, alt = -29.2543, -70.7346, 2335.0
        obsradius, R0 = G.JPLR0(lat, alt)
        obpos = G.obspos(lon, obsradius, R0)
        jplephem.set_observer_coordinates(float(obpos[0]), float(obpos[1]), float(obpos[2]))

        t = T.parse_target("tau Cet", TAU_CET)
        loc = EarthLocation.from_geodetic(lon * u.deg, lat * u.deg, alt * u.m)
        star = SkyCoord(ra=TAU_CET["ra"] * u.deg, dec=TAU_CET["dec"] * u.deg,
                        pm_ra_cosdec=TAU_CET["pmra"] * u.mas / u.yr, pm_dec=TAU_CET["pmdec"] * u.mas / u.yr,
                        distance=(1000 / TAU_CET["parallax"]) * u.pc, radial_velocity=TAU_CET["rv"] * u.km / u.s,
                        obstime=Time(2000.0, format="jyear", scale="tdb"))
        for mjd in (58000.3, 59500.1, 60247.65):
            obstime = Time(mjd, format="mjd", scale="utc")
            truth = star.apply_space_motion(new_obstime=obstime).radial_velocity_correction(
                kind="barycentric", obstime=obstime, location=loc).to(u.m / u.s).value
            ra, dec = T.position_at(t, mjd)
            frac = jplephem.doppler_fraction(ra / 15.0, dec, int(mjd), mjd % 1, 1, 0.0)["frac"][0]
            assert abs(frac * 299792458.0 - truth) < 0.1
            fixed = jplephem.doppler_fraction(TAU_CET["ra"] / 15.0, TAU_CET["dec"], int(mjd), mjd % 1, 1, 0.0)["frac"][0]
            assert abs(fixed * 299792458.0 - truth) > 0.5     # what the J2000 position got wrong


@pytest.fixture(scope="module")
def src() -> str:
    return ast.unparse(ast.parse(PIPE.read_text(), filename=str(PIPE)))


class TestWiring:
    def test_the_reffile_is_gone(self, src):
        assert "reffile" not in src
        assert "getcoords" not in src and "get_mask_reffile" not in src and "get_disp" not in src

    def test_the_targets_file_is_an_argument(self, src):
        assert "parser.add_argument('-targets'" in src
        assert "targets = targets_mod.load_targets(targets_path)" in src

    def test_the_position_is_propagated_before_the_barycentric_correction(self, src):
        prop = src.index("ra, dec = targets_mod.position_at(_target, mjd)")
        berv = src.index("jplephem.doppler_fraction(float(ra / 15.0)")
        assert prop < berv

    def test_the_mask_comes_from_the_target_or_defaults_to_g2(self, src):
        assert "sp_type, mask_source = (_target.mask, 'targets')" in src
        assert "sp_type, mask_source = ('G2', 'default')" in src

    def test_sources_are_recorded(self, src):
        assert src.count("'HIERARCH CERES COORD SOURCE', coord_source") == 2
        assert "'HIERARCH CERES MASK SOURCE', mask_source" in src
