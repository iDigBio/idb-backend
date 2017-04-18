import pytest
import os
from idigbio_ingestion.lib.eml import parseEml

@pytest.mark.usefixtures("emlpathdir")
@pytest.mark.parametrize("eml_filename,expected_license", [
        ("formatted.56e711e6-c847-4f99-915a-6894bb5c5dea_NHM_London.xml", "CC0"), # intellectualRights / para
        ("AEC-TTD-TCN_DwC-A20160308.eml", "CC4 BY"), # intellectualRights / para
        ("dr130.xml", "CC4 BY"), # intellectualRights / section | section / title | para
        ("dr367.xml", "CC4 BY-SA"),
        ("dr90.xml", "CC3 BY"),
        ("eml-bg_vascular-v4.66.xml", "CC4 BY"), # intellectualRights / para / ulink / citetitle
        ("formatted.Bohart-Tardigrada_DwC-A.eml", "CC3 BY-NC"),\
            # intellectualRights / para / ulink / broken citetitle, url is available in second intellectualRights
        ("invertnet_osu.eml.xml", "No license, assume Public Domain"), # no intellectualRights section
        ("MNHN_Paris_el.xml", "No license, assume Public Domain"), # no intellectualRights section
        ("MNHN_Paris_RA.xml", "CC4 BY"), # intellectualRights / para / <ulink> and <citetitle>
        ("museu_paraense_emilio_goeldi_ornithology_collection.xml", "Unknown License, assume Public Domain"), # Open Data Commons
        ("nmnh_extant_dwc-a.xml", "CC0"), # intellectualRights / para / <ulink> and <citetitle>
        ("tropicosspecimens.xml", "CC4 BY"), # intellectualRights / para / ulink / citetitle
        ("UWZM-F_DwC-A.eml", "CC0"), # intellectualRights / para / ulink / citetitle
        ("formatted.neherbaria.VT_DwC-A.eml", "CC3 BY-NC"), # broken citetitle, url is available in second intellectualRights
        ("formatted.mycoportal.VT_DwC-A.eml", "CC0"), # bare url in intellectualRights
        ("VT_DwC-A.eml", "CC3 BY-NC"), # intellectualRights / para / ulink / broken citetitle  ?
        ("rom_birdsnonpass.xml", "CC4 BY-NC"),
        ("vertnet_sui_verts.xml", "CC0"), # cc zero and vertnet norms
        #("usgs_pwrc_northamerican_bees", "No license, assume Public Domain"), # this is an html file that should not parse, currently raising an untrapped Exception
])


def test_intellectual_rights(eml_filename, expected_license, emlpathdir):
    emlfile = emlpathdir.join(eml_filename).open()
    parsed_eml = parseEml('id_placeholder_test_suite', emlfile.read())
    assert parsed_eml['data_rights'] == expected_license
