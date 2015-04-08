import unittest

import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))))

from helpers.conversions import *
import copy

class TestGetExponent(unittest.TestCase):
    def test_get_exponent(self):
        exponents = ["1", "0.1", "0.01", "0.001", "0.0001"]
        for i,exp in enumerate(exponents):
            self.assertEqual(i, getExponent(exp))

class TestSetFlags(unittest.TestCase):
    def test_set_flags(self):
        self.assertEqual([], setFlags({}))        

    def test_set_flags_returns_list_for_dict(self):
        self.assertEqual(['geopoint_similar_coord'], setFlags({'geopoint': [-38, 38]}))

    def test_set_flags_returns_list_for_dict_case_2(self):
        self.assertEqual(['geopoint_similar_coord', 'geopoint_0_coord'], setFlags({'geopoint': [0, 0]}))

    def test_set_flags_returns_list_for_dict_case_3(self):
        self.assertEqual(['datecollected_bounds'], setFlags({'datecollected': datetime.date(1000,1,2)}))

    def test_set_flags_returns_list_for_dict_case_4(self):
        self.assertEqual(['geopoint_0_coord'], setFlags({'geopoint': [1, 0]}))

    def test_set_flags_returns_list_for_dict_case_5(self):
        self.assertEqual(['geopoint_similar_coord'], setFlags({'geopoint': [38, 38]}))

    def test_set_flags_returns_list_for_dict_case_6(self):
        self.assertEqual(['datecollected_bounds'], setFlags({'datecollected': datetime.date(3000,1,2)}))

    def test_set_flags_returns_list_for_dict_case_7(self):
        self.assertEqual(['geopoint_0_coord'], setFlags({'geopoint': [0, 1]}))

class TestScore(unittest.TestCase):
    def test_score(self):
        self.assertEqual(0, score("records", {}))

    def test_score_returns_2_over_max_for_a_2_field_dict(self):
        self.assertEqual(2/maxscores["records"], score("records", {
            "institutioncode": "blah",
            "scientificname": "blah"
        }))

    def test_score_returns_0_for_a_2_field_dict_with_2_flags(self):
        self.assertEqual(0, score("records", {
            "institutioncode": "blah",
            "scientificname": "blah",
            "flags": ["blah", "blah"]
        }))

class TestVerbatimGrabber(unittest.TestCase):
    def test_verbatim_grabber(self):
        for t in fields.keys():
            for f in fields[t]:
                if f[1] != "":
                    if f[2] == "text" or f[2] == "longtext":
                        r = verbatimGrabber(t, {
                            f[1]: "blah"
                        })
                        self.assertEqual("blah", r[f[0]])
                    elif f[2] == "list":
                        r = verbatimGrabber(t, {
                            f[1]: ["blah"]
                        })
                        self.assertEqual(["blah"], r[f[0]])
                    else:
                        assert False

class TestGrabFirstNumber(unittest.TestCase):
    def test_grab_first_number(self):
        e = [
            ("2050","2050"),
            ("2050","2050 m"),
            ("2050.0","2050.0"),
            ("2050.0","2050.0 m"),
            ("2,050","2,050"),
            ("2,050","2,050 m"),
            ("2,050.0","2,050.0"),
            ("2,050.0","2,050.0 m"),
            ("100000","100000"),
            ("1000000","1000000"),
            ("100,000","100,000"),
            ("1,000,000","1,000,000"),
            ("100,000.0","100,000.0"),
            ("1,000,000.0","1,000,000.0")
        ]
        for n in e:
            self.assertEqual(n[0], grabFirstNumber(n[1]))

class TestMangleString(unittest.TestCase):
    def test_mangle_string(self):
        e = [
            ("BLAH","blah"),
            ("BLAH","blah "),
            ("BLAH"," blah "),
            ("BLAHBLAH","blah blah!"),
        ]
        for n in e:
            self.assertEqual(n[0], mangleString(n[1]))        

class TestGrabFirstUUID(unittest.TestCase):
    def test_grab_first_uui_d(self):
        e = [
            ("0072bf11-a354-4998-8730-c0cb4cfc9517","0072bf11-a354-4998-8730-c0cb4cfc9517"),
            ("0072bf11-a354-4998-8730-c0cb4cfc9517","blah 0072bf11-a354-4998-8730-c0cb4cfc9517 blah"),
            ("0072bf11-a354-4998-8730-c0cb4cfc9517","http://blah.blah/blah/0072bf11-a354-4998-8730-c0cb4cfc9517"),
        ]
        for n in e:
            self.assertEqual(n[0], grabFirstUUID(n[1]))        

class TestElevGrabber(unittest.TestCase):
    def test_elev_grabber(self):
        r = {
            "dwc:minimumElevationInMeters": "100.0 m",
            "dwc:maximumElevationInMeters": "100.0 m",
            "dwc:minimumDepthInMeters": "100.0 m",
            "dwc:maximumDepthInMeters": "100.0 m",
        }
        self.assertEqual({
            "minelevation": 100.0,
            "maxelevation": 100.0,
            "mindepth": 100.0,
            "maxdepth": 100.0,
        }, elevGrabber("records", r))

class TestIntGrabber(unittest.TestCase):
    def test_int_grabber(self):
        r = {
            "idigbio:version": "1",
        }
        self.assertEqual({
            "version": 1,
        }, intGrabber("records", r))
    def test_int_grabber_int(self):
        r = {
            "idigbio:version": 1,
        }
        self.assertEqual({
            "version": 1,
        }, intGrabber("records", r))
    def test_int_grabber_float(self):
        r = {
            "idigbio:version": 1.0,
        }
        self.assertEqual({
            "version": 1,
        }, intGrabber("records", r))


class TestGeoGrabber(unittest.TestCase):
    def test_geo_grabber(self):
        r = {
            "dwc:decimalLatitude": "34.567",
            "dwc:decimalLongitude": "134.567",
            "dwc:geodeticDatum": "WGS84"
        }
        self.assertEqual({'geopoint': (134.567, 34.567)}, geoGrabber("records", r))

class TestDateGrabber(unittest.TestCase):
    def test_date_grabber(self):
        r = {
            "idigbio:dateModified": "2014-01-10",
            "dwc:eventDate": "2014-01-10",
        }
        mr = {
            "idigbio:dateModified": "2014-01-10",
            "dcterms:modified": "2014-01-10",
        }
        self.assertEqual({
            "datemodified": datetime.date(2014,01,10),
            "datecollected": datetime.date(2014,01,10),
        }, dateGrabber("records", r))
        self.assertEqual({
            "datemodified": datetime.date(2014,01,10),
            "modified": datetime.date(2014,01,10),
        }, dateGrabber("mediarecords", mr))

    def test_date_grabber_year_month_day_fallback(self):
        r = {
            "idigbio:dateModified": "2014-01-10",
            "dwc:year": "2014",
            "dwc:month": "01",
            "dwc:day": "10",
        }
        self.assertEqual({
            "datemodified": datetime.date(2014,01,10),
            "datecollected": datetime.date(2014,01,10),
        }, dateGrabber("records", r))

class TestRelationsGrabber(unittest.TestCase):
    def test_relations_grabber(self):
        r = {
            "idigbio:links": {
                "recordset": [
                    "http://api.idigbio.org/v1/recordsets/0072bf11-a354-4998-8730-c0cb4cfc9517"
                ],
                "mediarecord": [
                    "http://api.idigbio.org/v1/mediarecords/ae175cc6-82f4-456b-910c-34da322e768d",
                    "http://api.idigbio.org/v1/mediarecords/d0ca23cd-d4eb-43b5-aaba-cb75f8aef9e3"
                ]
            }
        }
        mr = {
            "idigbio:links":{
                "recordset": [
                    "http://api.idigbio.org/v1/recordsets/40250f4d-7aa6-4fcc-ac38-2868fa4846bd"
                ],
                "record": [
                    "http://api.idigbio.org/v1/records/0000012b-9bb8-42f4-ad3b-c958cb22ae45"
                ]
            }
        }
        self.assertEqual({
            "hasImage": True,
            "mediarecords": [
                'ae175cc6-82f4-456b-910c-34da322e768d',
                'd0ca23cd-d4eb-43b5-aaba-cb75f8aef9e3'
            ],
            "recordset": '0072bf11-a354-4998-8730-c0cb4cfc9517',
        }, relationsGrabber("records", r))
        self.assertEqual({
            'hasSpecimen': True,
            'records': [
                '0000012b-9bb8-42f4-ad3b-c958cb22ae45'
            ],
            'recordset': '40250f4d-7aa6-4fcc-ac38-2868fa4846bd'
        }, relationsGrabber("mediarecords", mr))

class TestScientificNameFiller(unittest.TestCase):
    def test_scientific_name_filler(self):
        r = {
            "genus": "puma",
            "specificepithet": "concolor"
        }
        self.assertEqual("puma concolor", scientificNameFiller("records",r))
        

class TestGrabAll(unittest.TestCase):
    def test_grab_all(self):
        r = {
          "idigbio:uuid": "0000012b-9bb8-42f4-ad3b-c958cb22ae45",
          "idigbio:etag": "cb7d64ec3aef36fa4dec6a028b818e331a67aacc",
          "idigbio:dateModified": "2015-01-17T08:35:59.395Z",
          "idigbio:version": 5,
          "idigbio:createdBy": "872733a2-67a3-4c54-aa76-862735a5f334",
          "idigbio:data": {
            "dwc:startDayOfYear": "233",
            "dwc:specificEpithet": "monticola",
            "dwc:kingdom": "Plantae",
            "dwc:recordedBy": "P. Acevedo; A. Reilly",
            "dwc:locality": "Coral Bay Quarter, Bordeaux Mountain Road.",
            "dwc:order": "Myrtales",
            "dwc:habitat": "Sunny roadside.",
            "dwc:scientificNameAuthorship": "Hitchc.",
            "dwc:occurrenceID": "urn:uuid:ed400275-09d7-4302-b777-b4e0dcf7f2a3",
            "id": "762944",
            "dwc:stateProvince": "Saint John",
            "dwc:eventDate": "1987-08-21",
            "dwc:collectionID": "a2e32c87-d320-4a01-bafd-a9182ae2e191",
            "dwc:country": "U.S. Virgin Islands",
            "idigbio:recordId": "urn:uuid:ed400275-09d7-4302-b777-b4e0dcf7f2a3",
            "dwc:collectionCode": "Plants",
            "dwc:decimalLatitude": "18.348",
            "dwc:occurrenceRemarks": "Small tree. 3.0 m. Bark brown, stems smooth; flowers in buds yellow.",
            "dwc:basisOfRecord": "PreservedSpecimen",
            "dwc:genus": "Eugenia",
            "dwc:family": "Myrtaceae",
            "dc:rights": "http://creativecommons.org/licenses/by-nc-sa/3.0/",
            "dwc:identifiedBy": "Andrew Salywon, Jan 2003",
            "dwc:dynamicProperties": "Small tree. 3.0 m. Bark brown, stems smooth; flowers in buds yellow.",
            "symbiota:verbatimScientificName": "Eugenia monticola",
            "dwc:phylum": "Magnoliophyta",
            "dcterms:references": "http://swbiodiversity.org/seinet/collections/individual/index.php?occid=762944",
            "dwc:georeferenceSources": "georef batch tool 2012-07-09",
            "dwc:institutionCode": "ASU",
            "dwc:reproductiveCondition": "flowers",
            "dwc:georeferenceVerificationStatus": "reviewed - high confidence",
            "dwc:catalogNumber": "ASU0010142",
            "dwc:month": "8",
            "dwc:decimalLongitude": "-64.7131",
            "dwc:scientificName": "Eugenia monticola",
            "dwc:otherCatalogNumbers": "156664",
            "dwc:georeferencedBy": "jssharpe",
            "dwc:recordNumber": "1897",
            "dcterms:modified": "2012-07-09 12:00:09",
            "dwc:coordinateUncertaintyInMeters": "2000",
            "dwc:day": "21",
            "dwc:year": "1987"
          },
          "idigbio:links": {
            "recordset": [
              "http://api.idigbio.org/v1/recordsets/40250f4d-7aa6-4fcc-ac38-2868fa4846bd"
            ],
            "mediarecord": [
              "http://api.idigbio.org/v1/mediarecords/ae175cc6-82f4-456b-910c-34da322e768d",
              "http://api.idigbio.org/v1/mediarecords/d0ca23cd-d4eb-43b5-aaba-cb75f8aef9e3"
            ]
          },
          "idigbio:recordIds": [
            "urn:uuid:ed400275-09d7-4302-b777-b4e0dcf7f2a3"
          ]
        }
        e = {
            'barcodevalue': None,
            'basisofrecord': 'preservedspecimen',
            'bed': None,
            'catalognumber': 'asu0010142',
            'class': None,
            'collectioncode': 'plants',
            'collectionid': 'a2e32c87-d320-4a01-bafd-a9182ae2e191',
            'collectionname': None,
            'collector': 'p. acevedo; a. reilly',
            'commonname': None,
            'continent': None,
            'country': 'u.s. virgin islands',
            'countrycode': None,
            'county': None,
            'datecollected': datetime.date(1987, 8, 21),
            'datemodified': datetime.date(2015, 1, 17),
            'dqs': 0.35714285714285715,
            'earliestageorloweststage': None,
            'earliestepochorlowestseries': None,
            'earliesteraorlowesterathem': None,
            'earliestperiodorlowestsystem': None,
            'etag': 'cb7d64ec3aef36fa4dec6a028b818e331a67aacc',
            'eventdate': '1987-08-21',
            'family': 'myrtaceae',
            'fieldnumber': None,
            'flags': ['geopoint_datum_missing'],
            'formation': None,
            'genus': 'eugenia',
            'geopoint': (-64.7131, 18.348),
            'group': None,
            'hasImage': True,
            'highertaxon': None,
            'individualcount': None,
            'infraspecificepithet': None,
            'institutioncode': 'asu',
            'institutionid': None,
            'institutionname': None,
            'kingdom': 'plantae',
            'latestageorhigheststage': None,
            'latestepochorhighestseries': None,
            'latesteraorhighesterathem': None,
            'latestperiodorhighestsystem': None,
            'lithostratigraphicterms': None,
            'locality': 'coral bay quarter, bordeaux mountain road.',
            'lowestbiostratigraphiczone': None,
            'maxdepth': None,
            'maxelevation': None,
            'mediarecords': ['ae175cc6-82f4-456b-910c-34da322e768d',
                            'd0ca23cd-d4eb-43b5-aaba-cb75f8aef9e3'],
            'member': None,
            'mindepth': None,
            'minelevation': None,
            'municipality': None,
            'occurrenceid': 'urn:uuid:ed400275-09d7-4302-b777-b4e0dcf7f2a3',
            'order': 'myrtales',
            'phylum': 'magnoliophyta',
            'recordids': ['urn:uuid:ed400275-09d7-4302-b777-b4e0dcf7f2a3'],
            'recordnumber': '1897',
            'recordset': '40250f4d-7aa6-4fcc-ac38-2868fa4846bd',
            'scientificname': 'eugenia monticola',
            'specificepithet': 'monticola',
            'stateprovince': 'saint john',
            'typestatus': None,
            'uuid': '0000012b-9bb8-42f4-ad3b-c958cb22ae45',
            'verbatimeventdate': None,
            'verbatimlocality': None,
            'version': 5,
            'waterbody': None
        }
        d = copy.deepcopy(r["idigbio:data"])
        d.update(r)
        del d["idigbio:data"]
        # from pprint import pprint
        # pprint(grabAll("records", d))
        self.assertEqual(e, grabAll("records", d))

class TestGetfield(unittest.TestCase):
    def test_getfield(self):
        r = {
            "dwc:scientificName": "puma concolor",
            "dwc:institutionCode": "BLAH"
        }
        self.assertEqual("puma concolor", getfield("dwc:scientificName", r, "text"))

    def test_getfield_newformat(self):
        r = {
            "dwc:scientificname": "puma concolor",
            "dwc:institutioncode": "BLAH"
        }
        self.assertEqual("puma concolor", getfield("dwc:scientificName", r, "text"))        

    def test_getfield_lower_str(self):
        r = {
            "dwc:scientificName": "puma concolor",
            "dwc:institutionCode": "BLAH"
        }
        self.assertEqual("blah", getfield("dwc:institutionCode", r, "text"))
    
    def test_getfield_lower_unicode(self):
        r = {
            "dwc:scientificName": "puma concolor",
            "dwc:institutionCode": u"BLAH"
        }
        self.assertEqual("blah", getfield("dwc:institutionCode", r, "text"))

    def test_getfield_none_list(self):
        r = {
            "idigbio:recordIds": None
        }
        self.assertEqual(None, getfield("idigbio:recordIds", r, "list"))

class TestFloatGrabber(unittest.TestCase):
    def test_float_grabber(self):
        r = {
            "dwc:individualCount": "100.0"
        }
        self.assertEqual({
            "individualcount": 100.0
        }, floatGrabber("records", r))

if __name__ == '__main__':
    unittest.main()
