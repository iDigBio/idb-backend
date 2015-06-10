import unittest

import os
import sys
import pytz

sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))))

from helpers.query_shim import *
import copy


class TestIsString(unittest.TestCase):

    def test_is_string(self):
        truth = [u"test", "test"]
        for t in truth:
            assert isString(t)

        falsity = [{"a": "b"}, ["a", "b"], ("a", "b"), 1, 1.0, False]

        for f in falsity:
            assert not isString(f)


class TestExistsFilter(unittest.TestCase):

    def test_exists_filter(self):
        self.assertEqual(
            {"exists": {"field": "genus"}},
            existsFilter("genus")
        )


class TestMissingFilter(unittest.TestCase):

    def test_missing_filter(self):
        self.assertEqual(
            {"missing": {"field": "genus"}},
            missingFilter("genus")
        )


class TestTypeWrapper(unittest.TestCase):

    def test_type_wrapper(self):
        self.assertEqual(
            {"range": {"minelevation": {"gte": "100", "lte": "200"}}},
            typeWrapper("minelevation", "range", {
                "type": "range",
                "gte": "100",
                "lte": "200"
            })
        )


class TestRangeFilter(unittest.TestCase):

    def test_range_filter(self):
        self.assertEqual(
            {"range": {"minelevation": {"gte": "100", "lte": "200"}}},
            rangeFilter("minelevation", {
                "type": "range",
                "gte": "100",
                "lte": "200"
            })
        )


class TestPrefixFilter(unittest.TestCase):

    def test_prefix_filter(self):
        self.assertEqual(
            {"prefix": {"family": "aster"}},
            prefixFilter("family", {
                "type": "prefix",
                "value": "aster"
            })
        )


class TestGeoBoundingBox(unittest.TestCase):

    def test_geo_bounding_box(self):
        self.assertEqual(
            {"geo_bounding_box": {"geopoint": {"bottom_right": {
                "lat": -45.1119, "lon": 179.99999}, "top_left": {"lat": 19.23, "lon": -130}}}},
            geoBoundingBox("geopoint", {
                "type": "geo_bounding_box",
                "top_left": {
                    "lat": 19.23,
                    "lon": -130
                },
                "bottom_right": {
                    "lat": -45.1119,
                    "lon": 179.99999
                }
            })
        )


class TestGeoDistance(unittest.TestCase):

    def test_geo_distance(self):
        self.assertEqual(
            {"geo_distance": {
                "distance": "100km", "geopoint": {"lat": -46.3445, "lon": 110.454}}},
            geoDistance("geopoint", {
                "type": "geo_distance",
                "distance": "100km",
                "lat": -46.3445,
                "lon": 110.454
            })
        )


class TestTermFilter(unittest.TestCase):

    def test_term_filter(self):
        self.assertEqual(
            {"term": {"genus": "acer"}},
            termFilter("genus", "acer")
        )


class TestTermsFilter(unittest.TestCase):

    def test_terms_filter(self):
        self.assertEqual(
            {"terms": {"execution": "or", "genus": ["acer", "puma"]}},
            termsFilter("genus", ["acer", "puma"])
        )


class TestObjectType(unittest.TestCase):

    def test_object_type(self):
        assert True

    def test_object_type_exists(self):
        self.assertEqual(
            {"exists": {"field": "genus"}},
            objectType("genus", {
                "type": "exists",
            })
        )

    def test_object_type_missing(self):
        self.assertEqual(
            {"missing": {"field": "genus"}},
            objectType("genus", {
                "type": "missing",
            })
        )

    def test_object_type_range(self):
        self.assertEqual(
            {"range": {"genus": {"gte": "100", "lte": "200"}}},
            objectType("genus", {
                "type": "range",
                "gte": "100",
                "lte": "200"
            })
        )

    def test_object_type_geo_bounding_box(self):
        self.assertEqual(
            {"geo_bounding_box": {"genus": {"bottom_right": {
                "lat": -45.1119, "lon": 179.99999}, "top_left": {"lat": 19.23, "lon": -130}}}},
            objectType("genus", {
                "type": "geo_bounding_box",
                "top_left": {
                    "lat": 19.23,
                    "lon": -130
                },
                "bottom_right": {
                    "lat": -45.1119,
                    "lon": 179.99999
                }
            })
        )

    def test_object_type_geo_distance(self):
        self.assertEqual(
            {"geo_distance": {
                "distance": "100km", "genus": {"lat": -46.3445, "lon": 110.454}}},
            objectType("genus", {
                "type": "geo_distance",
                "distance": "100km",
                "lat": -46.3445,
                "lon": 110.454
            }))

    def test_object_type_fulltext(self):
        self.assertEqual(
            "aster",
            objectType("data", {
                "type": "fulltext",
                "value": "aster"
            })
        )


class TestQueryFromShim(unittest.TestCase):

    def test_query_from_shim(self):
        self.assertEqual(
            {"query": {"filtered": {"filter": {}}}},
            queryFromShim({})
        )

    def test_query_from_shim_fulltext(self):
        self.assertEqual(
            {"query": {"filtered": {"filter": {},
                                    "query": {"match": {"_all": {"operator": "and", "query": "aster"}}}}}},
            queryFromShim({"data": {
                "type": "fulltext",
                "value": "aster"
            }})
        )

    def test_query_from_shim_values_lower(self):
        self.assertEqual(
            {"query": {
                "filtered": {"filter": {"and": [{"term": {"genus": "acer"}}]}}}},
            queryFromShim({"genus": "acer"})
        )

    def test_query_from_shim_values_upper(self):
        self.assertEqual(
            {"query": {
                "filtered": {"filter": {"and": [{"term": {"genus": "acer"}}]}}}},
            queryFromShim({"genus": "Acer"})
        )

    def test_query_from_shim_values_number(self):
        self.assertEqual(
            {"query": {
                "filtered": {"filter": {"and": [{"term": {"version": 2}}]}}}},
            queryFromShim({"version": 2})
        )

    def test_query_from_shim_values_bool(self):
        self.assertEqual(
            {"query": {
                "filtered": {"filter": {"and": [{"term": {"hasImage": True}}]}}}},
            queryFromShim({"hasImage": True})
        )

    def test_query_from_shim_values_lower_list(self):
        self.assertEqual(
            {"query": {
                "filtered": {"filter": {"and": [{"terms": {"execution": "or", "genus": ["acer", "quercus"]}}]}}}},
            queryFromShim({"genus": ["acer", "quercus"]})
        )

    def test_query_from_shim_values_upper_list(self):
        self.assertEqual(
            {"query": {
                "filtered": {"filter": {"and": [{"terms": {"execution": "or", "genus": ["acer", "quercus"]}}]}}}},
            queryFromShim({"genus": ["Acer", "Quercus"]})
        )

    def test_query_from_shim_values_number_list(self):
        self.assertEqual(
            {"query": {
                "filtered": {"filter": {"and": [{"terms": {"execution": "or", "version": [2, 3]}}]}}}},
            queryFromShim({"version": [2, 3]})
        )

    def test_query_from_shim_values_bool_list(self):
        self.assertEqual(
            {"query": {
                "filtered": {"filter": {"and": [{"terms": {"execution": "or", "hasImage": [True, False]}}]}}}},
            queryFromShim({"hasImage": [True, False]})
        )

if __name__ == "__main__":
    unittest.main()
