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

from data_tables.rights_strings import acceptable_licenses_trans, licenses
PARENT_MAP = {
    "records": "recordsets",
    "mediarecords": "recordsets",
    "recordsets": "publishers",
}

locale.setlocale(locale.LC_ALL, '')

# [indexname, rawfield, type, include_in_max]
fields = {
    "records": [
        ["uuid","idigbio:uuid", "text", 0],
        ["datemodified","", "date", 0],
        ["etag", "idigbio:etag", "text", 0],
        ["recordids", "idigbio:recordIds", "list", 0],
        ["version", "", "integer", 0],
        ["kingdom","dwc:kingdom", "text", 1],
        ["phylum","dwc:phylum", "text", 1],
        ["class","dwc:class", "text", 1],
        ["order","dwc:order", "text", 1],
        ["family","dwc:family", "text", 1],
        ["genus","dwc:genus", "text", 1],
        ["basisofrecord","dwc:basisOfRecord", "text", 1],
        ["specificepithet","dwc:specificEpithet", "text", 1],
        ["infraspecificepithet","dwc:infraspecificEpithet", "text", 1],
        ["highertaxon","dwc:higherClassification", "longtext", 1],
        ["scientificname","dwc:scientificName", "text", 1],
        ["commonname","dwc:vernacularName", "text", 1],
        ["continent","dwc:continent", "text", 1],
        ["country","dwc:country", "text", 1],
        ["stateprovince","dwc:stateProvince", "text", 1],
        ["county","dwc:county", "text", 1],
        ["countrycode", "idigbio:isoCountryCode", "text", 1],
        ["municipality","dwc:municipality", "text", 1],
        ["waterbody","dwc:waterBody", "text", 1],
        ["locality","dwc:locality", "longtext", 1],
        ["verbatimlocality","dwc:verbatimLocality", "longtext", 1],
        ["geopoint","", "point", 1],
        ["minelevation","", "float", 1],
        ["maxelevation","", "float", 1],
        ["mindepth","", "float", 1],
        ["maxdepth","", "float", 1],
        ["datecollected","", "date", 1],
        ["institutionname","idigbio:institutionName", "text", 1],
        ["institutioncode","dwc:institutionCode", "text", 1],
        ["institutionid","dwc:institutionID", "text", 1],
        ["collectionname","idigbio:collectionName", "text", 1],
        ["collectioncode","dwc:collectionCode", "text", 1],
        ["collectionid","dwc:collectionID", "text", 1],
        ["occurrenceid","dwc:occurrenceID", "text", 1],
        ["barcodevalue","idigbio:barcodeValue", "text", 1],
        ["catalognumber","dwc:catalogNumber", "text", 1],
        ["fieldnumber","dwc:fieldNumber", "text", 1],
        ["recordnumber","dwc:recordNumber", "text", 1],
        ["typestatus","dwc:typeStatus", "text", 1],
        ["eventdate","dwc:eventDate", "text", 1],
        ["verbatimeventdate","dwc:verbatimEventDate", "text", 1],
        ["collector","dwc:recordedBy", "longtext", 1],
        ["recordset", "","text", 0],
        ["mediarecords", "", "list", 0],
        ["hasImage", "","boolean", 0],
        ["bed", "dwc:bed", "text", 1],
        ["group", "dwc:group", "text", 1],
        ["member", "dwc:member", "text", 1],
        ["formation", "dwc:formation", "text", 1],
        ["lowestbiostratigraphiczone", "dwc:lowestBiostratigraphicZone", "text", 1],
        ["lithostratigraphicterms", "dwc:lithostratigraphicTerms", "text", 1],
        ["earliestperiodorlowestsystem","dwc:earliestPeriodOrLowestSystem","text", 1],
        ["earliesteraorlowesterathem", "dwc:earliestEraOrLowestErathem", "text", 1],
        ["earliestepochorlowestseries", "dwc:earliestEpochOrLowestSeries", "text", 1],
        ["earliestageorloweststage", "dwc:earliestAgeOrLowestStage", "text", 1],
        ["latesteraorhighesterathem", "dwc:latestEraOrHighestErathem", "text", 1],
        ["latestepochorhighestseries", "dwc:latestEpochOrHighestSeries", "text", 1],
        ["latestageorhigheststage", "dwc:latestAgeOrHighestStage", "text", 1],
        ["latestperiodorhighestsystem","dwc:latestPeriodOrHighestSystem","text", 1],
        ["individualcount","","float", 0],
        ["flags", "", "list", 0],
        ["dqs", "", "float", 0],
    ],
    "mediarecords": [
        ["uuid","idigbio:uuid", "text", 0],
        ["datemodified","", "date", 0],
        ["modified","", "date", 1],
        ["etag", "idigbio:etag", "text", 0],
        ["version", "", "integer", 0],
        ["recordset", "","text", 0],
        ["records", "", "list", 0],
        ["format", "dc:format", "text", 1],
        ["type", "dc:type", "text", 1],
        ["tag", "ac:tag", "longtext", 1],
        ["xpixels","","integer",1],
        ["ypixels","","integer",1],
        ["rights", "", "text", 1],
        ["licenselogourl", "", "text", 1],
        ["webstatement", "", "text", 1],        
        ["hasSpecimen", "","boolean", 0],
        ["flags", "", "list", 0],
        ["dqs", "", "float", 0],
    ],
    "publishers": [
        ["uuid","idigbio:uuid", "text", 0],
        ["datemodified","", "date", 0],
        ["etag", "idigbio:etag", "text", 0],
        ["version", "", "integer", 0],
        ["flags", "", "list", 0],
        ["dqs", "", "float", 0],
        ["recordsets", "", "list", 0]
    ],
    "recordsets": [
        ["uuid","idigbio:uuid", "text", 0],
        ["datemodified","", "date", 0],
        ["etag", "idigbio:etag", "text", 0],
        ["version", "", "integer", 0],
        ["publisher", "","text", 0],
        ["flags", "", "list", 0],
        ["dqs", "", "float", 0],
    ]
}

maxscores = {}
for t in fields:
    maxscores[t] = 0.0
    for f in fields[t]:
        maxscores[t] += f[3]
    if maxscores[t] == 0.0:
        maxscores[t] = 1.0

def checkBounds(x):
    lowerBound = datetime.datetime(1700,1,2,tzinfo=pytz.utc)
    upperBound = datetime.datetime.now(pytz.utc)
    if isinstance(x,datetime.datetime):
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
        return -1*d.as_tuple().exponent
    except:
        return 0

def setFlags(d):
    flagset = []
    for k in flags.keys():
        if k in d and d[k] is not None:
            for f in flags[k].keys():
                if flags[k][f](d[k]):
                    flagset.append(k+"_"+f)
    return flagset

def score(t,d):
    scorenum = 0
    for f in fields[t]:
        if f[0] in d and d[f[0]] is not None:
            scorenum += f[3]
    if "flags" in d:
        scorenum -= len(d["flags"])
    return scorenum/maxscores[t]

def getfield(f,d,t="text"):
    fl = f.lower()
    if fl in d:
        f = fl
    if f in d and d[f] is not None:
        if t == "list":
            return [x.lower().strip() for x in d[f]]
        else:
            if isinstance(d[f],str) or isinstance(d[f],unicode):
                return d[f].lower().strip()
            else:
                return d[f]
    else:
        return None

def verbatimGrabber(t,d):
    r = {}
    for f in fields[t]:
        r[f[0]] = getfield(f[1],d,t=f[2])
    return r

gfn = re.compile("([+-]?[0-9]+(?:[,][0-9]{3})*(?:[\.][0-9]*)?)")
def grabFirstNumber(f):
    n = None
    try:
        if isinstance(f,int) or isinstance(f,float):
            n = f
        else:
            c = gfn.search(f)
            if c != None:
                n = c.groups()[0]
    except:
        pass
    return n

mangler = re.compile("[\W]+")
def mangleString(s):
    return mangler.sub('',s).upper()


uuid_re = re.compile("([a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12})")
def grabFirstUUID(f):
    n = None
    try:
        c = uuid_re.search(f)
        if c != None:
            n = c.groups()[0]
    except:
        pass
    return n

def elevGrabber(t,d):
    r = {}
    ef = {
        "records": [
            ["minelevation","dwc:minimumElevationInMeters"],
            ["maxelevation","dwc:maximumElevationInMeters"],
            ["mindepth","dwc:minimumDepthInMeters"],
            ["maxdepth","dwc:maximumDepthInMeters"],
        ]
    }
    if t in ef:
        for f in ef[t]:
            fv = getfield(f[1],d)
            if fv is not None:
                try:
                    n = grabFirstNumber(fv)
                    if n != None:
                        r[f[0]] = locale.atof(n)
                except:
                    pass
            if f[0] not in r:
                r[f[0]] = None
    return r

def intGrabber(t,d):
    r = {}
    ef = {
        "records": [
            ["version","idigbio:version"],
        ],
        "mediarecords": [
            ["version","idigbio:version"],
            ["xpixels","exif:PixelYDimension"],
            ["ypixels","exif:PixelXDimension"],
        ],
        "publishers": [
            ["version","idigbio:version"],
        ],
        "recordsets": [
            ["version","idigbio:version"],
        ]
    }
    for f in ef[t]:
        fv = getfield(f[1],d)
        if fv is not None:
            if isinstance(fv,(str,unicode)):
                try:
                    n = grabFirstNumber(fv)
                    if n != None:
                        r[f[0]] = locale.atoi(n)
                except:
                    pass
            elif isinstance(fv,int):
                r[f[0]] = fv
            elif isinstance(fv,float):
                r[f[0]] = int(fv)
        if f[0] not in r:
            r[f[0]] = None
    return r

def floatGrabber(t,d):
    r = {}
    ef = {
        "records": [
            ["individualcount","dwc:individualCount"],
        ],
        "mediarecords": [
        ],
        "publishers": [
        ],
        "recordsets": [
        ]
    }
    for f in ef[t]:
        fv = getfield(f[1],d)
        if fv is not None:
            try:
                n = grabFirstNumber(fv)
                if n != None:
                    r[f[0]] = locale.atof(n)
            except:
                pass
        if f[0] not in r:
            r[f[0]] = None
    return r

def geoGrabber(t,d):
    r = {}
    # get the lat and lon values
    lat_val = getfield("dwc:decimalLatitude",d)
    lon_val = getfield("dwc:decimalLongitude",d)

    # TODO: these falgs sould probably be set after the transform, but some of them may lose meaning
    # Ex. lower precision values may be converted to artifically more precise floats
    if lat_val is not None and lon_val is not None:
        try:
            latexp = getExponent(lat_val)
            lat = float(lat_val)
            if lat <= -90 or lat >= 90:
                r["geopoint"] = None
                r["flag_geopoint_bounds"] = True
                return r
            lonexp = getExponent(lon_val)
            lon = float(lon_val)
            if lon <= -180 or lon >= 180:
                r["geopoint"] = None
                r["flag_geopoint_bounds"] = True
                return r
            if latexp <= 2 or lonexp <=2:
                r["flag_geopoint_low_precision"] = True
            # set the geopoint to a lon,lat tuple
            r["geopoint"] = (lon,lat)
        except:
            r["geopoint"] = None
            #traceback.print_exc()

        # get the datum value
        datum_val = getfield("dwc:geodeticDatum",d)

        # if we got this far with actual values
        if r["geopoint"] is not None and datum_val is not None:
            # convert datum to a more canonical representation (no whitespace, all uppercase)
            source_datum = mangleString(datum_val)
            try:
                # source projection
                p1 = pyproj.Proj(proj="latlon", datum=source_datum)

                # destination projection
                p2 = pyproj.Proj(proj="latlon", datum="WGS84")

                # do the transform
                r["geopoint"] = pyproj.transform(p1,p2,r["geopoint"][0],r["geopoint"][1])
            except:
                # create an error flag on projection creation exception (invalid source datum)
                # or on transform exception (point out of bounds for source projection)
                r["flag_geopoint_datum_error"] = True
        elif r["geopoint"] is not None:
            # note unprojected points (datum_val is None)
            r["flag_geopoint_datum_missing"] = True
    return r

def dateGrabber(t,d):
    r = {}
    df = {
        "records": [
            ["datemodified","idigbio:dateModified"],
            ["datecollected","dwc:eventDate"],
        ],
        "mediarecords": [
            ["modified","dcterms:modified"],
            ["datemodified","idigbio:dateModified"],
        ],
        "publishers": [
            ["datemodified","idigbio:dateModified"],
        ],
        "recordsets": [
            ["datemodified","idigbio:dateModified"],
        ]
    }
    for f in df[t]:
        fv = getfield(f[1],d)
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

    if "datecollected" in r and r["datecollected"] == None:
        year = getfield("dwc:year",d)
        month = getfield("dwc:month",d)
        day = getfield("dwc:day",d)
        sd_of_year = getfield("dwc:startDayOfYear",d)
        if year is not None:
            try:
                if month is not None:
                    if day is not None:
                        r["datecollected"] = dateutil.parser.parse("{0}-{1}-{2}".format(year,month,day)).date()
                    elif sd_of_year is not None:
                        r["datecollected"] = (datetime.datetime(year, 1, 1) + datetime.timedelta(locale.atoi(sd_of_year) - 1)).date()
                    else:
                        r["datecollected"] = dateutil.parser.parse("{0}-{1}".format(year,month)).date()
                else:
                    r["datecollected"] = dateutil.parser.parse(year).date()
            except:
                pass

    return r

def relationsGrabber(t,d):
    df = {
        "records": [
            ["recordset", "recordset" , "text"],
            ["mediarecords", "mediarecord" , "list"],
        ],
        "mediarecords": [
            ["recordset", "recordset" ,"text"],
            ["records", "record", "list"],
        ],
        "publishers": [
            ["recordsets", "recordset", "list"],
        ],
        "recordsets": [
            ["publisher", "publisher" ,"text"],
        ]
    }
    r = {}
    if "idigbio:links" in d:
        for f in df[t]:
            if f[1] in d["idigbio:links"]:
                if f[2] == "text":
                    r[f[0]] = grabFirstUUID(d["idigbio:links"][f[1]][0])
                elif f[2] == "list":
                    r[f[0]] = [grabFirstUUID(x) for x in d["idigbio:links"][f[1]] if grabFirstUUID(x) is not None]
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
        r["hasSpecimen"] = "records" in r and r["records"] != None
    elif t == "records":
        r["hasImage"] = "mediarecords" in r and r["mediarecords"] != None

    return r

def getLicense(t,d):
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



def scientificNameFiller(t,r):
    sciname = None
    if "scientificname" not in r or r["scientificname"] is None:
        if "genus" in r and r["genus"] is not None:
            sciname = r["genus"]
            if "specificepithet" in r and r["specificepithet"] is not None:
                sciname += " " + r["specificepithet"]
    elif "scientificname" in r:
        sciname = r["scientificname"]
    return sciname

def grabAll(t,d):
    r = verbatimGrabber(t,d)
    r.update(elevGrabber(t,d))
    r.update(intGrabber(t,d))
    r.update(floatGrabber(t,d))
    r.update(geoGrabber(t,d))
    r.update(dateGrabber(t,d))
    r.update(relationsGrabber(t,d))
    r.update(getLicense(t,d))
    # Done with non-dependant fields.
    r["scientificname"] = scientificNameFiller(t,r)

    r["flags"] = setFlags(r)
    for k in r.keys():
        if k.startswith("flag_"):
            r["flags"].append("_".join(k.split("_")[1:]))
            del r[k]
    for k in d.keys():
        if k.startswith("flag_"):
            r["flags"].append("_".join(k.split("_")[1:]))

    r["dqs"] = score(t,r)

    return r

def main():
    d = {
        "xmpRights:WebStatement": "http://creativecommons.org/licenses/by-nc/4.0/",
        "ac:hashValue": "265a83b255bf0eca18e45aa5b6876c13",
        "dc:type": "StillImage",
        "idigbio:associatedRecordsetReference": "285a4be0-5cfe-4d4f-9c8b-b0f0f3571079",
        "xmpRights:UsageTerms": "CC BY-NC",
        "idigbio:recordId": "TTD_TCN_MICH_1361582.jpg",
        "ac:licenseLogoURL": "http://mirrors.creativecommons.org/presskit/buttons/80x15/png/by-nc.png",
        "ac:hashFunction": "MD5",
        "dcterms:format": "image/jpeg",
        "ac:accessURI": "http://media.idigbio.org/lookup/images/265a83b255bf0eca18e45aa5b6876c13",
        "dcterms:modified": "2014-09-09 19:02:50.640000"
    }
    print getLicense("mediarecords",d)

if __name__ == '__main__':
    main()