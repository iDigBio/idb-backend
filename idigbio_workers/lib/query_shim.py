# Updates to this file SHOULD also be reflected into https://github.com/iDigBio/idigbio-search-api/blob/master/src/lib/query-shim.js
# Last reviewed 2025-12-02 against https://github.com/iDigBio/idigbio-search-api/blob/0191b904f32d350fd5c79894e6022bac2b457a05/src/lib/query-shim.js

from __future__ import division, absolute_import, print_function
import copy

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
    #Don't allow invalid coordinates to be passed to ES
    if shimK["top_left"]["lat"] < shimK["bottom_right"]["lat"]:
        temp = shimK["top_left"]["lat"]
        shimK["top_left"]["lat"] = shimK["bottom_right"]["lat"]
        shimK["bottom_right"]["lat"] = temp
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
    return typeWrapper(k, "geo_shape", {"shape": shimK["value"]})


def geoPolygon(k, shimK):
    return typeWrapper(k, "geo_polygon", {"points": shimK["value"]})


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


def exactFilter(k, shimK): # handler for "exact" toggle on portal UI.
    term = dict()
    term[k+'.exact'] = shimK['text']

    if isinstance(shimK['text'], list):
        #noop: also unused in idigbio-search-api:query-shim.js
        #map(lambda s: s.lower(), term[k+'.exact'])

        return { "terms": term }  # use "terms" plural when passing an array of terms.
    else:
        term[k+'.exact'] = term[k+'.exact'].lower()
        return { "term": term }


def keywordFilter(k, shimK):
    term = dict()
    term[k+'.keyword'] = shimK['text']

    if isinstance(shimK['text'], list):
        #noop: also unused in idigbio-search-api:query-shim.js
        #map(lambda s: s.lower(), term[k+'.keyword'])

        return { "terms": term }
    else:
        term[k+'.keyword'] = term[k+'.keyword'].lower()
        return { "term": term }


def fuzzyFilter(k, shimK):
    queryParam = shimK['text']
    esQuery = dict()
    fuzziness = "AUTO" if shimK['type'] else 0 # type is only present when fuzzy is toggled to true
    if fuzziness == "AUTO":
        k += ".fuzzy"
    if isinstance(shimK['text'], list): # combine multiple terms into a bool query
        matchQueries = map(lambda param: { "match": { k: {
                "query": param.lower(),
                "operator": "and",
                "fuzziness": fuzziness
            }}}, queryParam)
        esQuery = { "query": { "bool": { "should": matchQueries }}}
    elif isinstance(shimK, list):
        matchQueries = map(lambda param: { "match": { k: {
                "query": param.lower(),
                "operator": "and",
                "fuzziness": fuzziness
            }}}, shimK)
        esQuery = { "query": { "bool": { "should": matchQueries }}} # should will OR the contents of the array to determine hits
    else: # single term
        esQuery = { "match": { k: {
            "query": (queryParam.lower()
                if queryParam
                else shimK.lower()),
            "operator": "and",
            "fuzziness": fuzziness
        }}}
    return esQuery


def objectType(k, shimK):
    if shimK["type"] == "exists":
        return existsFilter(k)
    elif shimK["type"] == "missing":
        return missingFilter(k)
    elif shimK["type"] == "exact":
        return exactFilter(k, shimK)
    elif shimK["type"] == "keyword":
        return keywordFilter(k, shimK)
    elif shimK["type"] == "fuzzy":
        return fuzzyFilter(k, shimK)
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
        if k == 'scientificname': # TODO: Add support for other fields, store a map containing their keys
            return fuzzyFilter(k, shimK)
        else:
            return termFilter(k, shimK)
    elif isinstance(shimK, list):
        if k == 'scientificname':
            return fuzzyFilter(k, shimK)
        else:
            return termsFilter(k, shimK)
    else:
        try:
            if "type" in shimK:
                return objectType(k, shimK)
            else:
                raise QueryParseExcpetion("unable to get type")
        except Exception:
            logger.exception("Failure in singleFilter", k + " " + repr(shimK))

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
