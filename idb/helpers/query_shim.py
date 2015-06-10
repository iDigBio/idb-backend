import copy


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
        return shimK["value"].lower()
    elif shimK["type"] == "prefix":
        return prefixFilter(k, shimK)
    else:
        raise UnknownTypeException(shimK["type"])


def queryFromShim(shim, term_type=None):
    if term_type is not None:
        # Term validation not yet implemented in python
        pass

    query = {
        "query": {
            "filtered": {
                "filter": {}
            }
        }
    }

    fulltext = None
    and_array = []

    for k in shim:
        if isString(shim[k]) or isinstance(shim[k], bool) or isinstance(shim[k], int) or isinstance(shim[k], float):
            and_array.append(termFilter(k, shim[k]))
        elif isinstance(shim[k], list):
            and_array.append(termsFilter(k, shim[k]))
        else:
            try:
                if "type" in shim[k]:
                    f = objectType(k, shim[k])
                    if isString(f):
                        fulltext = f
                    else:
                        and_array.append(f)
                else:
                    raise QueryParseExcpetion("unable to get type")
            except:
                logger.error(traceback.format_exc())
                logger.error(k + " " + shim[k])

    if fulltext is not None:
        query["query"]["filtered"]["query"] = {
            "match": {
                "_all": {
                    "query": fulltext,
                    "operator": "and"
                }
            }
        }

    if len(and_array) > 0:
        query["query"]["filtered"]["filter"]["and"] = and_array

    return query
