from __future__ import division, absolute_import, print_function
import copy

import traceback
import logging
logger = logging.getLogger()

class UnknownTypeException(Exception):
    pass


class QueryParseExcpetion(Exception):
    pass


class TermNotFoundExcpetion(Exception):
    pass


def isString(v):
    return isinstance(v, str) or isinstance(v, unicode)


def existsFilter(k):
    return {
        "exists": {
            "field": k,
        }
    }


def missingFilter(k):
    return {
        "missing": {
            "field": k,
        }
    }


def typeWrapper(k, t, shimK):
    qd = copy.deepcopy(shimK)
    del qd["type"]
    return {
        t: {
            k: qd
        }
    }


def rangeFilter(k, shimK):
    return typeWrapper(k, "range", shimK)


def prefixFilter(k, shimK):
    if isString(shimK["value"]):
        return {
            "prefix": {
                k: shimK["value"].lower()
            }
        }
    else:
        return {
            "prefix": {
                k: shimK["value"]
            }
        }


def geoBoundingBox(k, shimK):
    return typeWrapper(k, "geo_bounding_box", shimK)


def geoDistance(k, shimK):
    qd = copy.deepcopy(shimK)
    del qd["type"]
    d = qd["distance"]
    del qd["distance"]
    return {
        "geo_distance": {
            "distance": d,
            k: qd
        }
    }


def geoShape(k, shimK):
    return typeWrapper(k, "geo_shape", {"shape": shimK})


def geoPolygon(k, shimK):
    return typeWrapper(k, "geo_polygon", {"points": shimK})


def termFilter(k, shimK):
    if isString(shimK):
        return {
            "term": {
                k: shimK.lower()
            }
        }
    else:
        return {
            "term": {
                k: shimK
            }
        }


def termsFilter(k, shimK):
    or_array = []
    for v in shimK:
        if isString(v):
            or_array.append(v.lower())
        else:
            or_array.append(v)

    return {
        "terms": {
            "execution": "or",
            k: or_array
        }
    }

def queryFilter(k, shimK):
    return {
        "query": {
            "match": {
                "_all": {
                    "query": shimK["value"].lower(),
                    "operator": "and"
                }
            }
        }
    }

def objectType(k, shimK):
    if shimK["type"] == "exists":
        return existsFilter(k)
    elif shimK["type"] == "missing":
        return missingFilter(k)
    elif shimK["type"] == "range":
        return rangeFilter(k, shimK)
    elif shimK["type"] == "geo_bounding_box":
        return geoBoundingBox(k, shimK)
    elif shimK["type"] == "geo_distance":
        return geoDistance(k, shimK)
    elif shimK["type"] == "fulltext":
        return queryFilter(k, shimK)
    elif shimK["type"] == "prefix":
        return prefixFilter(k, shimK)
    elif shimK["type"] == "geo_shape":
        return geoShape(k, shimK)
    elif shimK["type"] == "geo_polygon":
        return geoPolygon(k, shimK)
    else:
        raise UnknownTypeException(shimK["type"])

def singleFilter(k, shimK):
    if isString(shimK) or isinstance(shimK, bool) or isinstance(shimK, int) or isinstance(shimK, float):
        return termFilter(k, shimK)
    elif isinstance(shimK, list):
        return termsFilter(k, shimK)
    else:
        try:
            if "type" in shimK:
                return objectType(k, shimK)
            else:
                raise QueryParseExcpetion("unable to get type")
        except:
            logger.error(traceback.format_exc())
            logger.error(k + " " + repr(shimK))

def andFilter(shim):
    and_array = []

    for k in shim:
        and_array.append(singleFilter(k, shim[k]))

    return {
        "and": and_array
    }

def queryFromShim(shim, term_type=None):
    if term_type is not None:
        # Term validation not yet implemented in python
        pass

    query = {
        "query": {
            "filtered": {
                "filter": andFilter(shim)
            }
        }
    }

    if len(query["query"]["filtered"]["filter"]["and"]) == 0:
        query["query"]["filtered"]["filter"] = {}

    return query
