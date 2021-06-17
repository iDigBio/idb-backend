import pytest
from copy import deepcopy
from idigbio_ingestion.lib.dwca import get_unescaped_fieldsEnclosedBy
from idigbio_ingestion.lib.dwca import get_unescaped_fieldsTerminatedBy
from idigbio_ingestion.lib.dwca import get_unescaped_linesTerminatedBy


# sample extracted during processing of recordset c38b867b-05f3-4733-802e-d8d2d3324f84
FILEDICT_BASE = {
    'files': {'location': 'occurrence.txt'},
    '#linesTerminatedBy': '\\n',
    '#ignoreHeaderLines': '1',
    '#fieldsEnclosedBy': '',
    'id': {'#index': '0'},
    'field': [
        {'#index': '1', '#term': 'http://purl.org/dc/terms/modified'},
        {'#index': '2', '#term': 'http://rs.tdwg.org/dwc/terms/institutionCode'},
        {'#index': '3', '#term': 'http://rs.tdwg.org/dwc/terms/collectionCode'},
        {'#index': '4', '#term': 'http://rs.tdwg.org/dwc/terms/basisOfRecord'},
        {'#index': '5', '#term': 'http://rs.tdwg.org/dwc/terms/occurrenceID'},
        {'#index': '6', '#term': 'http://rs.tdwg.org/dwc/terms/catalogNumber'},
        {'#index': '7', '#term': 'http://rs.tdwg.org/dwc/terms/recordedBy'},
        {'#index': '8', '#term': 'http://rs.tdwg.org/dwc/terms/preparations'},
        {'#index': '9', '#term': 'http://rs.tdwg.org/dwc/terms/otherCatalogNumbers'}, {'#index': '10', '#term': 'http://rs.tdwg.org/dwc/terms/eventDate'}, {'#index': '11', '#term': 'http://rs.tdwg.org/dwc/terms/year'}, {'#index': '12', '#term': 'http://rs.tdwg.org/dwc/terms/month'}, {'#index': '13', '#term': 'http://rs.tdwg.org/dwc/terms/day'}, {'#index': '14', '#term': 'http://rs.tdwg.org/dwc/terms/fieldNumber'}, {'#index': '15', '#term': 'http://rs.tdwg.org/dwc/terms/eventRemarks'}, {'#index': '16', '#term': 'http://rs.tdwg.org/dwc/terms/higherGeography'}, {'#index': '17', '#term': 'http://rs.tdwg.org/dwc/terms/continent'}, {'#index': '18', '#term': 'http://rs.tdwg.org/dwc/terms/country'}, {'#index': '19', '#term': 'http://rs.tdwg.org/dwc/terms/stateProvince'}, {'#index': '20', '#term': 'http://rs.tdwg.org/dwc/terms/county'}, {'#index': '21', '#term': 'http://rs.tdwg.org/dwc/terms/locality'}, {'#index': '22', '#term': 'http://rs.tdwg.org/dwc/terms/minimumDepthInMeters'}, {'#index': '23', '#term': 'http://rs.tdwg.org/dwc/terms/maximumDepthInMeters'}, {'#index': '24', '#term': 'http://rs.tdwg.org/dwc/terms/decimalLatitude'}, {'#index': '25', '#term': 'http://rs.tdwg.org/dwc/terms/decimalLongitude'}, {'#index': '26', '#term': 'http://rs.tdwg.org/dwc/terms/geodeticDatum'}, {'#index': '27', '#term': 'http://rs.tdwg.org/dwc/terms/georeferencedDate'}, {'#index': '28', '#term': 'http://rs.tdwg.org/dwc/terms/georeferenceProtocol'}, {'#index': '29', '#term': 'http://rs.tdwg.org/dwc/terms/identificationQualifier'}, {'#index': '30', '#term': 'http://rs.tdwg.org/dwc/terms/typeStatus'}, {'#index': '31', '#term': 'http://rs.tdwg.org/dwc/terms/scientificName'}, {'#index': '32', '#term': 'http://rs.tdwg.org/dwc/terms/kingdom'}, {'#index': '33', '#term': 'http://rs.tdwg.org/dwc/terms/phylum'}, {'#index': '34', '#term': 'http://rs.tdwg.org/dwc/terms/class'}, {'#index': '35', '#term': 'http://rs.tdwg.org/dwc/terms/order'}, {'#index': '36', '#term': 'http://rs.tdwg.org/dwc/terms/family'}, {'#index': '37', '#term': 'http://rs.tdwg.org/dwc/terms/genus'}, {'#index': '38', '#term': 'http://rs.tdwg.org/dwc/terms/specificEpithet'}, {'#index': '39', '#term': 'http://rs.tdwg.org/dwc/terms/infraspecificEpithet'}
    ],
    '#fieldsTerminatedBy': '\\t',
    '#encoding': 'UTF-8',
    '#rowType': 'http://rs.tdwg.org/dwc/terms/Occurrence'}

@pytest.fixture
def filedict_fieldsEnclosedBy_nothing():
    filedict = deepcopy(FILEDICT_BASE)
    return filedict

@pytest.fixture
def filedict_fieldsEnclosedBy_escapeddoublequotes():
    filedict = deepcopy(FILEDICT_BASE)
    filedict['#fieldsEnclosedBy'] = '\"'
    return filedict

@pytest.fixture
def filedict_fieldsTerminatedBy_tab():
    filedict = deepcopy(FILEDICT_BASE)
    filedict['#fieldsTerminatedBy'] = '\\t'
    return filedict

def test_get_unescaped_linesTerminatedBy_from_filedict():
    assert(get_unescaped_linesTerminatedBy(FILEDICT_BASE) == '\n')

def test_get_unescaped_fieldsTerminatedBy_from_filedict(filedict_fieldsTerminatedBy_tab):
    assert(get_unescaped_fieldsTerminatedBy(filedict_fieldsTerminatedBy_tab) == '\t')

def test_get_unescaped_fieldsEnclosedBy_from_filedict(filedict_fieldsEnclosedBy_escapeddoublequotes):
    assert(get_unescaped_fieldsEnclosedBy(filedict_fieldsEnclosedBy_escapeddoublequotes) == '"')


@pytest.fixture
def dwca_schema():
    # sample xml schema document goes here!
    # or read from the data dir.
    return "xmldoc something something"

@pytest.mark.skip(reason="dwca.py needs refactoring")
def test_dwca_schema_parse():
    assert(False)


@pytest.mark.skip(reason="dwca.py needs refactoring")
def test_something_about_finding_extensions():
    assert(False)
