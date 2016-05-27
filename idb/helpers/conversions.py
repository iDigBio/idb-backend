from __future__ import absolute_import
import dateutil.parser
import re
import pprint
import traceback
import locale
import decimal
import datetime
import pytz
import pyproj
import string
from shapely import wkt
from shapely.geometry import Polygon, mapping


from idb.data_tables.rights_strings import acceptable_licenses_trans, licenses
from idb.data_tables.locality_data import iso_two_to_three

from .biodiversity_socket_connector import Biodiversity
from idb.helpers.rg import get_country


b = Biodiversity()


PARENT_MAP = {
    "records": "recordsets",
    "mediarecords": "recordsets",
    "recordsets": "publishers",
}

mime_mapping = {
    "image/jpeg": "images",
    "image/jp2": "images",
    "text/html": None,
    "image/dng": None,
    "application/xml": None,
    "image/x-adobe-dng": None,
    "audio/mpeg3": "sounds",
    "audio/mpeg": "sounds",
    "video/mpeg": "video",
    "video/mp4": "video",
    "text/html": None,
    "model/mesh": "models",
    None: None
}

unmapped_buckets = { 'datasets', 'debugfile' }

valid_buckets = set([v for v in mime_mapping.values() if v is not None]) | \
                unmapped_buckets

locale.setlocale(locale.LC_ALL, '')

# [indexname, rawfield, type, include_in_max]
fields = {
    "records": [
        ["uuid", "idigbio:uuid", "text", 0, None],
        ["datemodified", "", "date", 0, "idigbio:dateModified"],
        ["etag", "idigbio:etag", "text", 0, None],
        ["recordids", "idigbio:recordIds", "list", 0, None],
        ["version", "", "integer", 0, "idigbio:version"],
        ["kingdom", "dwc:kingdom", "text", 1, None],
        ["phylum", "dwc:phylum", "text", 1, None],
        ["class", "dwc:class", "text", 1, None],
        ["order", "dwc:order", "text", 1, None],
        ["family", "dwc:family", "text", 1, None],
        ["genus", "dwc:genus", "text", 1, None],
        ["basisofrecord", "dwc:basisOfRecord", "text", 1, None],
        ["specificepithet", "dwc:specificEpithet", "text", 1, None],
        ["infraspecificepithet", "dwc:infraspecificEpithet", "text", 1, None],
        ["highertaxon", "dwc:higherClassification", "longtext", 1, None],
        ["scientificname", "dwc:scientificName", "text", 1, None],
        ["commonname", "dwc:vernacularName", "text", 1, None],
        ["continent", "dwc:continent", "text", 1, None],
        ["country", "dwc:country", "text", 1, None],
        ["stateprovince", "dwc:stateProvince", "text", 1, None],
        ["county", "dwc:county", "text", 1, None],
        ["countrycode", "idigbio:isoCountryCode", "text", 1, None],
        ["municipality", "dwc:municipality", "text", 1, None],
        ["waterbody", "dwc:waterBody", "text", 1, None],
        ["locality", "dwc:locality", "longtext", 1, None],
        ["verbatimlocality", "dwc:verbatimLocality", "longtext", 1, None],
        ["geopoint", "", "point", 1, "idigbio:geoPoint"],
        ["geoshape", "", "shape", 1, "idigbio:geoShape"],
        ["minelevation", "", "float", 1, "dwc:minimumElevationInMeters"],
        ["maxelevation", "", "float", 1, "dwc:maximumElevationInMeters"],
        ["mindepth", "", "float", 1, "dwc:minimumDepthInMeters"],
        ["maxdepth", "", "float", 1, "dwc:maximumDepthInMeters"],
        ["coordinateuncertainty", "", "float", 1,
            "dwc:coordinateUncertaintyInMeters"],
        ["datecollected", "", "date", 1, "dwc:eventDate"],
        ["startdayofyear", "", "integer", 1, "dwc:startDayOfYear"],
        ["institutionname", "idigbio:institutionName", "text", 1, None],
        ["institutioncode", "dwc:institutionCode", "text", 1, None],
        ["institutionid", "dwc:institutionID", "text", 1, None],
        ["collectionname", "idigbio:collectionName", "text", 1, None],
        ["collectioncode", "dwc:collectionCode", "text", 1, None],
        ["collectionid", "dwc:collectionID", "text", 1, None],
        ["occurrenceid", "dwc:occurrenceID", "text", 1, None],
        ["barcodevalue", "idigbio:barcodeValue", "text", 1, None],
        ["catalognumber", "dwc:catalogNumber", "text", 1, None],
        ["fieldnumber", "dwc:fieldNumber", "text", 1, None],
        ["recordnumber", "dwc:recordNumber", "text", 1, None],
        ["typestatus", "dwc:typeStatus", "text", 1, None],
        ["eventdate", "dwc:eventDate", "text", 1, None],
        ["verbatimeventdate", "dwc:verbatimEventDate", "text", 1, None],
        ["collector", "dwc:recordedBy", "longtext", 1, None],
        ["recordset", "", "text", 0, "idigbio:recordset"],
        ["mediarecords", "", "list", 0, "idigbio:mediarecords"],
        ["hasImage", "", "boolean", 0, "idigbio:hasImage"],
        ["hasMedia", "", "boolean", 0, "idigbio:hasMedia"],
        ["bed", "dwc:bed", "text", 1, None],
        ["group", "dwc:group", "text", 1, None],
        ["member", "dwc:member", "text", 1, None],
        ["formation", "dwc:formation", "text", 1, None],
        ["lowestbiostratigraphiczone",
            "dwc:lowestBiostratigraphicZone", "text", 1, None],
        ["lithostratigraphicterms",
            "dwc:lithostratigraphicTerms", "text", 1, None],
        ["earliestperiodorlowestsystem",
            "dwc:earliestPeriodOrLowestSystem", "text", 1, None],
        ["earliesteraorlowesterathem",
            "dwc:earliestEraOrLowestErathem", "text", 1, None],
        ["earliestepochorlowestseries",
            "dwc:earliestEpochOrLowestSeries", "text", 1, None],
        ["earliestageorloweststage",
            "dwc:earliestAgeOrLowestStage", "text", 1, None],
        ["latesteraorhighesterathem",
            "dwc:latestEraOrHighestErathem", "text", 1, None],
        ["latestepochorhighestseries",
            "dwc:latestEpochOrHighestSeries", "text", 1, None],
        ["latestageorhigheststage",
            "dwc:latestAgeOrHighestStage", "text", 1, None],
        ["latestperiodorhighestsystem",
            "dwc:latestPeriodOrHighestSystem", "text", 1, None],
        ["individualcount", "", "float", 0, "dwc:individualCount"],
        ["flags", "", "list", 0, "idigbio:flags"],
        ["dqs", "", "float", 0, "idigbio:dataQualityScore"],
        ["gbif_cannonicalname", "gbif:cannonicalName", "text", 1, None],
        ["gbif_genus", "gbif:genus", "text", 1, None],
        ["gbif_specificepithet", "gbif:specificEpithet", "text", 1, None],
        ["gbif_taxonid", "gbif:taxonID", "text", 1, None],
    ],
    "mediarecords": [
        ["uuid", "idigbio:uuid", "text", 0, None],
        ["datemodified", "", "date", 0, "idigbio:dateModified"],
        ["modified", "", "date", 1, "dcterms:modified"],
        ["etag", "idigbio:etag", "text", 0, None],
        ["version", "", "integer", 0, "idigbio:version"],
        ["recordids", "idigbio:recordIds", "list", 0, None],
        ["recordset", "", "text", 0, "idigbio:recordsets"],
        ["records", "", "list", 0, "idigbio:records"],
        ["format", "", "text", 1, "dcterms:format"],
        ["mediatype", "", "text", 1, "idigbio:mediaType"],
        ["type", "dc:type", "text", 1, None],
        ["tag", "ac:tag", "longtext", 1, None],
        ["accessuri", "", "text", 1, "ac:accessURI"],
        ["xpixels", "", "integer", 1, "exif:PixelXDimension"],
        ["ypixels", "", "integer", 1, "exif:PixelYDimension"],
        ["rights", "", "text", 1, "dcterms:rights"],
        ["licenselogourl", "", "text", 1, "ac:licenseLogoURL"],
        ["webstatement", "", "text", 1, "xmpRights:WebStatement"],
        ["hasSpecimen", "", "boolean", 0, "idigbio:hasSpecimen"],
        ["flags", "", "list", 0, "idigbio:flags"],
        ["dqs", "", "float", 0, "idigbio:dataQualityScore"],
    ],
    "publishers": [
        ["uuid", "idigbio:uuid", "text", 0, None],
        ["datemodified", "", "date", 0, "idigbio:dateModified"],
        ["etag", "idigbio:etag", "text", 0, None],
        ["version", "", "integer", 0, "idigbio:version"],
        ["recordids", "idigbio:recordIds", "list", 0, None],
        ["flags", "", "list", 0, "idigbio:flags"],
        ["dqs", "", "float", 0, "idigbio:dataQualityScore"],
        ["recordsets", "", "list", 0, "idigbio:recordsets"],
        ["name", "name", "text", 0, "idigbio:publisherName"],
    ],
    "recordsets": [
        ["uuid", "idigbio:uuid", "text", 0, None],
        ["datemodified", "", "date", 0, "idigbio:dateModified"],
        ["etag", "idigbio:etag", "text", 0, None],
        ["version", "", "integer", 0, "idigbio:version"],
        ["recordids", "idigbio:recordIds", "list", 0, None],
        ["publisher", "", "text", 0, "idigbio:publisher"],
        ["flags", "", "list", 0, "idigbio:flags"],
        ["dqs", "", "float", 0, "idigbio:dataQualityScore"],
        ["rights", "data_rights", "text", 0, "dcterms:rights"],
        ["contacts", "contacts", "custom", 0, None],
        ["archivelink", "link", "text", 0, "idigbio:archiveLink"],
        ["emllink", "eml_link", "text", 0, "idigbio:emlLink"],
        ["logourl", "logo_url", "text", 0, "idigbio:logoUrl"],
        ["name", "collection_name", "text", 0, "dwc:datasetName"],
    ]
}

custom_mappings = {
    "recordsets": {
        "contacts": {
            "type": "nested",
            "include_in_parent": True,
            "properties": {
                "first_name": {"type": "string", "analyzer": "keyword"},
                "last_name": {"type": "string", "analyzer": "keyword"},
                "email": {"type": "string", "analyzer": "keyword"},
                "role": {"type": "string", "analyzer": "keyword"},
            }
        }
    }
}

index_field_to_longname = {}
for t in fields.keys():
    index_field_to_longname[t] = {}
    for f in fields[t]:
        longname = f[4]
        if longname is None:
            if f[1] != "":
                longname = f[1]
            else:
                longname = "idigbio:" + f[0]
        index_field_to_longname[t][f[0]] = longname


maxscores = {}
for t in fields:
    maxscores[t] = 0.0
    for f in fields[t]:
        maxscores[t] += f[3]
    if maxscores[t] == 0.0:
        maxscores[t] = 1.0


def checkBounds(x):
    lowerBound = datetime.datetime(1700, 1, 2, tzinfo=pytz.utc)
    upperBound = datetime.datetime.now(pytz.utc)
    if isinstance(x, datetime.datetime):
        return x < lowerBound or x > upperBound
    else:
        return x < lowerBound.date() or x > upperBound.date()

flags = {
    "geopoint": {
        "0_coord": lambda x: x[0] == 0 or x[1] == 0,
        "similar_coord": lambda x: abs(x[0]) == abs(x[1]),
    },
    "datecollected": {
        "bounds": checkBounds
    }
}


def getExponent(fs):
    try:
        d = decimal.Decimal(fs)
        return -1 * d.as_tuple().exponent
    except:
        return 0


def setFlags(d):
    flagset = []
    for k in flags.keys():
        if k in d and d[k] is not None:
            for f in flags[k].keys():
                if flags[k][f](d[k]):
                    flagset.append(k + "_" + f)
    return flagset


def score(t, d):
    scorenum = 0
    for f in fields[t]:
        if f[0] in d and d[f[0]] is not None:
            scorenum += f[3]
    if "flags" in d:
        scorenum -= len(d["flags"])
    return scorenum / maxscores[t]


def getfield(f, d, t="text"):
    fl = f.lower()
    if fl in d:
        f = fl
    if f in d and d[f] is not None:
        if t == "list":
            return [x.lower().strip() for x in d[f]]
        else:
            if isinstance(d[f], str) or isinstance(d[f], unicode):
                return d[f].lower().strip()
            else:
                return d[f]
    else:
        return None


def verbatimGrabber(t, d):
    r = {}
    for f in fields[t]:
        r[f[0]] = getfield(f[1], d, t=f[2])
    return r

gfn = re.compile("([+-]?[0-9]+(?:[,][0-9]{3})*(?:[\.][0-9]*)?)")


def grabFirstNumber(f):
    n = None
    try:
        if isinstance(f, int) or isinstance(f, float):
            n = f
        else:
            c = gfn.search(f)
            if c is not None:
                n = c.groups()[0]
    except:
        pass
    return n

mangler = re.compile("[\W]+")


def mangleString(s):
    return mangler.sub('', s).upper()


uuid_re = re.compile(
    "([a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12})")


def grabFirstUUID(f):
    n = None
    try:
        c = uuid_re.search(f)
        if c is not None:
            n = c.groups()[0]
    except:
        pass
    return n


def elevGrabber(t, d):
    r = {}
    ef = {
        "records": [
            ["minelevation", "dwc:minimumElevationInMeters"],
            ["maxelevation", "dwc:maximumElevationInMeters"],
            ["mindepth", "dwc:minimumDepthInMeters"],
            ["maxdepth", "dwc:maximumDepthInMeters"],
        ]
    }
    if t in ef:
        for f in ef[t]:
            fv = getfield(f[1], d)
            if fv is not None:
                try:
                    n = grabFirstNumber(fv)
                    if n is not None:
                        r[f[0]] = locale.atof(n)
                except:
                    pass
            if f[0] not in r:
                r[f[0]] = None
    return r


def intGrabber(t, d):
    r = {}
    ef = {
        "records": [
            ["version", "idigbio:version"],
        ],
        "mediarecords": [
            ["version", "idigbio:version"],
            ["xpixels", "exif:PixelYDimension"],
            ["ypixels", "exif:PixelXDimension"],
        ],
        "publishers": [
            ["version", "idigbio:version"],
        ],
        "recordsets": [
            ["version", "idigbio:version"],
        ]
    }
    for f in ef[t]:
        fv = getfield(f[1], d)
        if fv is not None:
            if isinstance(fv, (str, unicode)):
                try:
                    n = grabFirstNumber(fv)
                    if n is not None:
                        r[f[0]] = locale.atoi(n)
                except:
                    pass
            elif isinstance(fv, int):
                r[f[0]] = fv
            elif isinstance(fv, float):
                r[f[0]] = int(fv)
        if f[0] not in r:
            r[f[0]] = None
    return r


def floatGrabber(t, d):
    r = {}
    ef = {
        "records": [
            ("individualcount", "dwc:individualCount"),
            ("coordinateuncertainty", "dwc:coordinateUncertaintyInMeters"),
        ],
        "mediarecords": [
        ],
        "publishers": [
        ],
        "recordsets": [
        ]
    }
    for resultkey, fieldkey in ef[t]:
        fv = getfield(fieldkey, d)
        if fv is not None:
            try:
                n = grabFirstNumber(fv)
                if n is not None:
                    r[resultkey] = locale.atof(n)
            except:
                pass
        if resultkey not in r:
            r[resultkey] = None
    return r


def geoGrabber(t, d):
    r = {}
    # get the lat and lon values
    lat_val = getfield("dwc:decimalLatitude", d)
    lon_val = getfield("dwc:decimalLongitude", d)

    if lat_val is not None and lon_val is not None:
        try:
            lat = float(lat_val)
            lon = float(lon_val)

            latexp = getExponent(lat_val)
            lonexp = getExponent(lon_val)

            if (
                (-180 <= lat < -90 or 90 < lat <= 180) and
                (-90 <= lon <= 90)
            ):
                lat, lon = lon, lat
                r["flag_geopoint_pre_flip"] = True

            if not (-90 <= lat <= 90):
                r["geopoint"] = None
                r["flag_geopoint_bounds"] = True
                return r

            if not (-180 <= lon <= 180):
                r["geopoint"] = None
                r["flag_geopoint_bounds"] = True
                return r
            if latexp <= 2 or lonexp <= 2:
                r["flag_geopoint_low_precision"] = True
            # set the geopoint to a lon,lat tuple
            r["geopoint"] = (lon, lat)
        except:
            r["geopoint"] = None
            # traceback.print_exc()

        # get the datum value
        datum_val = getfield("dwc:geodeticDatum", d)

        # if we got this far with actual values
        if r["geopoint"] is not None:
            if datum_val is not None:
                # convert datum to a more canonical representation (no
                # whitespace, all uppercase)
                source_datum = mangleString(datum_val)
                try:
                    # source projection
                    p1 = pyproj.Proj(proj="latlon", datum=source_datum)

                    # destination projection
                    p2 = pyproj.Proj(proj="latlon", datum="WGS84")

                    # do the transform
                    # (lon, lat)
                    r["geopoint"] = pyproj.transform(
                        p1, p2, r["geopoint"][0], r["geopoint"][1])
                except:
                    # traceback.print_exc()
                    # create an error flag on projection creation exception (invalid source datum)
                    # or on transform exception (point out of bounds for source
                    # projection)
                    r["flag_geopoint_datum_error"] = True
            else:
                # note unprojected points (datum_val is None)
                r["flag_geopoint_datum_missing"] = True

            # get_country takes lon, lat
            result = get_country(r["geopoint"][0], r["geopoint"][1], eez=False)
            if result is None:
                result_eez = get_country(r["geopoint"][0], r["geopoint"][1], eez=True)
                if result_eez is not None:
                    result = result_eez
                    r["flag_rev_geocode_eez"] = True

            test_flips = False
            if result is None:
                r["flag_rev_geocode_failure"] = True
                test_flips = True
            elif filled("idigbio:isocountrycode", d) and result.lower() != d["idigbio:isocountrycode"]:
                r["flag_rev_geocode_mismatch"] = True
                test_flips = True

            if filled("idigbio:isocountrycode", d) and test_flips:
                r["flag_rev_geocode_mismatch"] = True
                flip_queries = [  # Point, "Distance" from original coords, Flag
                    [(-r["geopoint"][0], r["geopoint"][1]),
                     1, "rev_geocode_lon_sign"],
                    [(r["geopoint"][0], -r["geopoint"][1]),
                     1, "rev_geocode_lat_sign"],
                    [(-r["geopoint"][0], -r["geopoint"][1]),
                     2, "rev_geocode_both_sign"],
                ]
                if abs(r["geopoint"][0]) <= 90.0:
                    flip_queries.extend([
                        [(r["geopoint"][1], r["geopoint"][0]),
                         2, "rev_geocode_flip"],
                        [(-r["geopoint"][1], r["geopoint"][0]),
                         3, "rev_geocode_flip_lat_sign"],
                        [(r["geopoint"][1], -r["geopoint"][0]),
                         3, "rev_geocode_flip_lon_sign"],
                        [(-r["geopoint"][1], -r["geopoint"][0]),
                         4, "rev_geocode_flip_both_sign"]
                    ])
                results = [get_country(*f[0], eez=False) for f in flip_queries] + \
                          [get_country(*f[0], eez=True) for f in flip_queries]
                for i, f in enumerate(results):
                    if f is not None and f.lower() == d["idigbio:isocountrycode"]:
                        # Flip back to lon, lat
                        real_i = i % len(flip_queries)
                        r["geopoint"] = (
                            flip_queries[real_i][0][0], flip_queries[real_i][0][1])
                        # Set flag
                        r["flag_" + flip_queries[real_i][2]] = True
                        if real_i != i:
                            r["flag_rev_geocode_eez_corrected"] = True
                        r["flag_rev_geocode_corrected"] = True
                        break
    return r


def dateGrabber(t, d):
    r = {}
    df = {
        "records": [
            ["datemodified", "idigbio:dateModified"],
            ["datecollected", "dwc:eventDate"],
        ],
        "mediarecords": [
            ["modified", "dcterms:modified"],
            ["datemodified", "idigbio:dateModified"],
        ],
        "publishers": [
            ["datemodified", "idigbio:dateModified"],
        ],
        "recordsets": [
            ["datemodified", "idigbio:dateModified"],
        ]
    }
    for f in df[t]:
        fv = getfield(f[1], d)
        if fv is not None:
            # dates are more sensitivie to lower case then upper.
            fv = fv.upper()
            try:
                x = dateutil.parser.parse(fv)
                if x.tzinfo is None:
                    x = x.replace(tzinfo=pytz.utc)
                try:
                    x < datetime.datetime.now(pytz.utc)
                except:
                    x = x.replace(tzinfo=pytz.utc)
                r[f[0]] = x
            except:
                pass
        if f[0] not in r:
            r[f[0]] = None

    if "datecollected" in r and r["datecollected"] is None:
        year = getfield("dwc:year", d)
        month = getfield("dwc:month", d)
        day = getfield("dwc:day", d)
        sd_of_year = getfield("dwc:startDayOfYear", d)
        if year is not None:
            try:
                if month is not None:
                    if day is not None:
                        r["datecollected"] = dateutil.parser.parse(
                            "{0}-{1}-{2}".format(year, month, day)).date()
                    elif sd_of_year is not None:
                        r["datecollected"] = (datetime.datetime(
                            year, 1, 1) + datetime.timedelta(locale.atoi(sd_of_year) - 1)).date()
                    else:
                        r["datecollected"] = dateutil.parser.parse(
                            "{0}-{1}".format(year, month)).date()
                else:
                    r["datecollected"] = dateutil.parser.parse(year).date()
            except:
                pass

    if "datecollected" in r and r["datecollected"] is not None:
        r["startdayofyear"] = r["datecollected"].timetuple().tm_yday

    return r


def relationsGrabber(t, d):
    df = {
        "records": [
            ["recordset", "recordset", "text"],
            ["mediarecords", "mediarecord", "list"],
        ],
        "mediarecords": [
            ["recordset", "recordset", "text"],
            ["records", "record", "list"],
        ],
        "publishers": [
            ["recordsets", "recordset", "list"],
        ],
        "recordsets": [
            ["publisher", "publisher", "text"],
        ]
    }
    r = {}
    if "idigbio:links" in d:
        for f in df[t]:
            if f[1] in d["idigbio:links"]:
                if f[2] == "text":
                    r[f[0]] = grabFirstUUID(d["idigbio:links"][f[1]][0])
                elif f[2] == "list":
                    r[f[0]] = [grabFirstUUID(x) for x in d["idigbio:links"][
                        f[1]] if grabFirstUUID(x) is not None]
            else:
                r[f[0]] = None
    elif "idigbio:siblings" in d:
        for f in df[t]:
            if f[1] in d["idigbio:siblings"]:
                if f[2] == "text":
                    r[f[0]] = d["idigbio:siblings"][f[1]][0]
                elif f[2] == "list":
                    r[f[0]] = [x for x in d["idigbio:siblings"][f[1]]]
            else:
                r[f[0]] = None

    if "idigbio:parent" in d:
        if t in PARENT_MAP:
            r["".join(PARENT_MAP[t][:-1])] = d["idigbio:parent"]

    if t == "mediarecords":
        r["hasSpecimen"] = "records" in r and r["records"] is not None
    elif t == "records":
        r["hasImage"] = "mediarecords" in r and r["mediarecords"] is not None
        r["hasMedia"] = "mediarecords" in r and r["mediarecords"] is not None

    return r


def getLicense(t, d):
    df = {
        "records": [
        ],
        "mediarecords": [
            "dcterms:rights",
            "dc:rights",
            "xmpRights:UsageTerms",
            "xmpRights:WebStatement",
            "dcterms:license"
        ],
        "publishers": [
        ],
        "recordsets": [
        ]
    }
    l = []
    for f in df[t]:
        if f in d:
            if d[f] in acceptable_licenses_trans:
                l.append(acceptable_licenses_trans[d[f]])
    if len(l) > 0:
        most_common_lic = max(set(l), key=l.count)
        return licenses[most_common_lic]
    else:
        return {}


def get_accessuri(t, d):
    #return k in d and d[k] is not None
    url = d.get("ac:accessURI") or d.get("ac:bestQualityAccessURI")
    if url is None:
        # Don't use identifier as a url for things that supply audubon core properties
        for k in d.keys():
            if k.startswith("ac:"):
                break
        else:
            url = d.get("dcterms:identifier") or d.get("dc:identifier")

    return {"accessuri": url}


def get_media_type(t, d):
    form = d.get("dcterms:format") or d.get("dc:format") or d.get("ac:bestQualityFormat")
    if form:
        form = form.strip()
        t = mime_mapping.get(form)

    return {
        "format": form,
        "mediatype": t
    }


def filled(k, d):
    return k in d and d[k] is not None


def genusSpeciesFiller(t, r):
    gs = b.get_genus_species(r["scientificname"])
    return gs


def scientificNameFiller(t, r):
    sciname = None
    if filled("genus", r):
        sciname = r["genus"]
        if filled("specificepithet", r):
            sciname += " " + r["specificepithet"]
    return sciname


def gs_sn_crossfill(t, r):
    if filled("genus", r) and not filled("scientificname", r):
        r["scientificname"] = scientificNameFiller(t, r)
        r["flag_scientificname_added"] = True
    elif filled("scientificname", r) and not filled("genus", r):
        gs = genusSpeciesFiller(t, r)
        for k, indk in [("genus", "genus"), ("species", "specificepithet")]:
            if filled(k, gs) and not filled(indk, r):
                r[indk] = gs[k]
                r["flag_" + indk + "_added"] = True


def generate_geoshape_from_wkt(t, d):
    r = {}
    poly = None
    try:
        poly = wkt.loads(d["dwc:footprintWKT"])
    except:
        try:
            wkta = [float(c) for c in d["dwc:footprintWKT"].split(",")]
            if len(wkta) > 1 and len(wkta) % 2 == 0:
                poly = Polygon(zip(wkta[::2], wkta[1::2]))
            else:
                r["flag_geoshape_invalid_wkt"] = True
        except ValueError:
            # Failed Float Conversion
            r["flag_geoshape_invalid_wkt"] = True
        except:
            r["flag_geoshape_invalid_wkt"] = True

    if poly is not None:
        r["geoshape"] = mapping(poly)
    return r


def generate_geoshape_from_point_radius(t, r):
    return {
        "geoshape": {
            "type": "circle",
            "coordinates": r["geopoint"],
            "radius": str(r["coordinateuncertainty"])
        }
    }


def geoshape_fill(t, d, r):
    resp = {}
    if filled("dwc:footprintWKT", d):
        resp.update(generate_geoshape_from_wkt(t, d))

    if (
        "geoshape" not in resp and
        filled("geopoint", r) and
        filled("coordinateuncertainty", r)
    ):
        resp.update(generate_geoshape_from_point_radius(t, r))

    return resp


def fixBOR(t, r):
    if filled("basisofrecord", r):
        if "preserved" in r["basisofrecord"]:
            r["basisofrecord"] = "preservedspecimen"
        elif "fossil" in r["basisofrecord"]:
            r["basisofrecord"] = "fossilspecimen"
        elif "living" in r["basisofrecord"]:
            r["basisofrecord"] = "livingspecimen"
        elif "specimen" in r["basisofrecord"]:
            r["basisofrecord"] = "preservedspecimen"
        elif "machine" in r["basisofrecord"] and "observation" in r["basisofrecord"]:
            r["basisofrecord"] = "machineobservation"
        elif "observation" in r["basisofrecord"]:
            r["basisofrecord"] = "humanobservation"
        else:
            r["basisofrecord"] = None
            r["flag_dwc_basisofrecord_removed"] = True
            r["flag_dwc_basisofrecord_invalid"] = True

        if r["basisofrecord"] == "preservedspecimen":
            paleo_terms = [
                "bed",
                "group",
                "member",
                "formation",
                "lowestbiostratigraphiczone",
                "lithostratigraphicterms",
                "earliestperiodorlowestsystem",
                "earliesteraorlowesterathem",
                "earliestepochorlowestseries",
                "earliestageorloweststage",
                "latesteraorhighesterathem",
                "latestepochorhighestseries",
                "latestageorhigheststage",
                "latestperiodorhighestsystem",
            ]

            for f in paleo_terms:
                if filled(f,r):
                    r["flag_dwc_basisofrecord_paleo_conflict"] = True
                    r["flag_dwc_basisofrecord_replaced"] = True
                    r["basisofrecord"] = "fossilspecimen"
                    break

# Step, count, ms, ms/count     action
# rc 1000 354.179 0.354179      record corrector
# 0 1000 44.898 0.044898        r = verbatimGrabber(t, d)
# 1 1000 18.476 0.018476        r.update(elevGrabber(t, d))
# 2 1000 28.539 0.028539        r.update(intGrabber(t, d))
# 3 1000 12.228 0.012228        r.update(floatGrabber(t, d))
# 4 1000 285.278 0.285278       r.update(geoGrabber(t, d)) # 4
# 5 1000 397.866 0.397866       r.update(dateGrabber(t, d)) # 5
# 6 1000 55.907 0.055907        r.update(relationsGrabber(t, d))
# 7 1000 14.834 0.014834        r.update(getLicense(t, d))
# 8 1000 10.355 0.010355        gs_sn_crossfill(t, r)
# 9 1000 11.182 0.011182        r.update(geoshape_fill(t, d, r))
# 10 1000 14.917 0.014917       r["flags"] = setFlags(r)
# 11 1000 16.59 0.01659         r flag loop
# 12 1000 54.133 0.054133       d flag loop
# 13 1000 15.618 0.015618       r["dqs"] = score(t, r)

def grabAll(t, d):
    r = verbatimGrabber(t, d)
    r.update(elevGrabber(t, d))
    r.update(intGrabber(t, d))
    r.update(floatGrabber(t, d))
    r.update(geoGrabber(t, d))  # 4
    r.update(dateGrabber(t, d))  # 5
    r.update(relationsGrabber(t, d))
    r.update(getLicense(t, d))
    r.update(get_media_type(t, d))
    r.update(get_accessuri(t, d))
    # Done with non-dependant fields.

    gs_sn_crossfill(t, r)
    fixBOR(t, r)

    # Disable geoshape for now, it uses a ton of space
    # r.update(geoshape_fill(t, d, r))

    r["flags"] = setFlags(r)
    for k in r.keys():
        if k.startswith("flag_"):
            r["flags"].append("_".join(k.split("_")[1:]))
            del r[k]
    for k in d.keys():
        if k.startswith("flag_"):
            r["flags"].append("_".join(k.split("_")[1:]))
    r["dqs"] = score(t, r)

    return r


def main():
    def dt_serial(obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        else:
            return obj

    import sys
    import requests
    import copy
    import json
    from corrections.record_corrector import RecordCorrector

    rc = RecordCorrector()

    s = requests.Session()

    retry_count = 3
    obj = None
    while retry_count > 0:
        try:
            resp = s.get("http://api.idigbio.org/v1/records/?limit=10000")
            resp.raise_for_status()
        except:
            retry_count -= 1
            continue
        break
    obj = resp.json()

    recs = []
    for li in obj["idigbio:items"]:
        retry_count = 3
        while retry_count > 0:
            try:
                rec_resp = s.get(
                    "http://api.idigbio.org/v1/{0}/{1}".format("records", li["idigbio:uuid"]))
                rec_resp.raise_for_status()
                recs.append(rec_resp.json())
            except:
                retry_count -= 1
                continue
            break

    print "records ready"

    interations = 1

    total_time = 0.0
    count = 0

    for _ in range(0, interations):
        for rec in recs:
            t1 = datetime.datetime.now()
            d, _ = rc.correct_record(rec["idigbio:data"])
            d.update(rec)
            del d["idigbio:data"]
            r = grabAll(t, d)
            t2 = datetime.datetime.now()
            total_time += (t2 - t1).total_seconds()
            count += 1

    print count, total_time, total_time / count

    # t = sys.argv[1]
    # u = sys.argv[2]
    # r = requests.get("http://api.idigbio.org/v1/{0}/{1}".format(t, u))
    # r.raise_for_status()
    # o = r.json()
    # d, _ = rc.correct_record(o["idigbio:data"])
    # d.update(o)
    # del d["idigbio:data"]
    # print json.dumps(d, indent=2)
    # print json.dumps(grabAll(t, d), default=dt_serial, indent=2)

if __name__ == '__main__':
    main()
