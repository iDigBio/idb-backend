import pytest
import os
from idigbio_ingestion.lib.eml import parseEml

#  Consider adding tests for uncovered cases:
#    "CC3 BY-NC"
#    ""
#    ""

@pytest.mark.usefixtures("emlpathdir")
@pytest.mark.parametrize("eml_filename,expected_license", [
        ("formatted.56e711e6-c847-4f99-915a-6894bb5c5dea_NHM_London.xml", "CC0"), # intellectualRights / para
        ("AEC-TTD-TCN_DwC-A20160308.eml", "CC4 BY"), # intellectualRights / para
        # ("dr130.xml", "CC4 BY"), # intellectualRights / section | section / title | para
        ("eml-bg_vascular-v4.66.xml", "CC4 BY"), # intellectualRights / para / ulink / citetitle
        # ("formatted.Bohart-Tardigrada_DwC-A.eml", "BY-NC"),\
            # intellectualRights / para / ulink / broken citetitle, url is available in second intellectualRights
        ("invertnet_osu.eml.xml", "No license, assume Public Domain"), # no intellectualRights section
        ("MNHN_Paris_el.xml", "No license, assume Public Domain"), # no intellectualRights section
        ("MNHN_Paris_RA.xml", "CC4 BY"), # intellectualRights / para / <ulink> and <citetitle>
        ("museu_paraense_emilio_goeldi_ornithology_collection.xml", "Unknown License, assume Public Domain"), # Open Data Commons
        ("nmnh_extant_dwc-a.xml", "CC0"), # intellectualRights / para / <ulink> and <citetitle>
        ("tropicosspecimens.xml", "CC4 BY"), # intellectualRights / para / ulink / citetitle
        ("UWZM-F_DwC-A.eml", "CC0"), # intellectualRights / para / ulink / citetitle
        # ("formatted.neherbaria.VT_DwC-A.eml", "BY-NC"), # broken citetitle, url is available in second intellectualRights
        ("formatted.mycoportal.VT_DwC-A.eml", "CC0"), # bare url in intellectualRights
        # ("VT_DwC-A.eml", "BY-NC"), # intellectualRights / para / ulink / broken citetitle  ?
])


def test_intellectual_rights(eml_filename, expected_license, emlpathdir):
    emlfile = emlpathdir.join(eml_filename).open()
    parsed_eml = parseEml('id_placeholder_test_suite', emlfile.read())
    assert parsed_eml['data_rights'] == expected_license
#    assert file.exists()


###############################
# example from http://stackoverflow.com/questions/17434031/py-test-parametrizing-test-classes

# ts = range(2000, 20001, 1000)  # This creates a list of numbers from 2000 to 20000 in increments of 1000.

# @pytest.fixture(params=ts)
# def plasma(request):
#     return plasma.LTEPlasma.from_abundance(request.param, {'Si':1.0}, 1e-13, atom_data, 10*86400)


# class TestNormalLTEPlasma:

#     def test_beta_rad(self, plasma):
#         assert plasma.beta_rad == 1 / (10000 * constants.k_B.cgs.value)

#     def test_t_electron(self, plasma):
#         assert plasma.t_electron == 0.9 * plasma.t_rad

#     def test_saha_calculation_method(self, plasma):
#         assert plasma.calculate_saha == plasma.calculate_saha_lte


#######

# the main function of eml.py returns parsed eml as json.

# $ python eml.py  "../../tests/data/eml/Bohart-Tardigrada_DwC-A.eml"
# {"contacts": [{"email": "birdbrain13@gmail.com"}, {"first_name": "Bohart Museum of Entomology", "email": "bmuseum@ucdavis.edu"}], "collection_name": "Bohart Museum", "institution_web_address": "http://bohart.ucdavis.edu/", "collection_description": "", "id": "testid", "other_guids": [], "logo_url": "http://symbiota4.acis.ufl.edu/tardigrade/portal/content/collicon/bohart-tardigrada.jpg", "data_rights": "No license, assume Public Domain"}

