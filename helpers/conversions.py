import dateutil.parser
import re
import pprint
import traceback
import locale
import decimal
import datetime
import pyproj
import string

locale.setlocale(locale.LC_ALL, '')

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
        ["earliestperiodorlowestsystem","dwc:earliestPeriodOrLowestSystem","text", 1],
        ["latestperiodorhighestsystem","dwc:latestPeriodOrHighestSystem","text", 1],
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

flags = {
    "geopoint": {
        "0_coord": lambda x: x[0] == 0 or x[1] == 0,
        "similar_coord": lambda x: abs(x[0]) == abs(x[1]),
    },
    "datecollected": {
        "bounds": lambda x: x < datetime.date(1700,1,2) or x > datetime.date.today()
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
    scorenum -= len(d["flags"])
    return scorenum/maxscores[t]

def verbatimGrabber(t,d):    
    r = {}
    for f in fields[t]:
        if f[1] in d:
            if f[2] == "list":
                r[f[0]] = [x.lower().strip() for x in d[f[1]]]
            else:
                r[f[0]] = d[f[1]].lower().strip()
        else:
            r[f[0]] = None
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
            if f[1] in d:
                try:
                    n = grabFirstNumber(d[f[1]])
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
        ],
        "publishers": [
            ["version","idigbio:version"],
        ],
        "recordsets": [
            ["version","idigbio:version"],
        ]
    }
    for f in ef[t]:
        if f[1] in d:
            try:
                n = grabFirstNumber(d[f[1]])
                if n != None:
                    r[f[0]] = locale.atoi(n)                
            except:
                pass
        if f[0] not in r:
            r[f[0]] = None        
    return r

def geoGrabber(t,d):
    r = {}
    if "dwc:decimalLatitude" in d and "dwc:decimalLongitude" in d:
        try:
            latexp = getExponent(d["dwc:decimalLatitude"])            
            lat = float(d["dwc:decimalLatitude"])
            if lat <= -90 or lat >= 90:
                r["geopoint"] = None
                r["flag_geopoint_bounds"] = True
                return r
            lonexp = getExponent(d["dwc:decimalLongitude"])
            lon = float(d["dwc:decimalLongitude"])
            if lon <= -180 or lon >= 180:
                r["geopoint"] = None
                r["flag_geopoint_bounds"] = True
                return r
            if latexp <= 2 or lonexp <=2:
                r["flag_geopoint_low_precision"] = True
            r["geopoint"] = (lon,lat)
        except:
            r["geopoint"] = None
            #traceback.print_exc()

        if r["geopoint"] is not None and "dwc:geodeticDatum" in d:
            source_datum = mangleString(d["dwc:geodeticDatum"])
            try:
                p1 = pyproj.Proj(proj="latlon", datum=source_datum)
                p2 = pyproj.Proj(proj="latlon", datum="WGS84")
                r["geopoint"] = pyproj.transform(p1,p2,r["geopoint"][0],r["geopoint"][1])
            except:
                r["flag_geopoint_datum_error"] = True
        elif r["geopoint"] is not None:
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
        if f[1] in d:
            try:
                r[f[0]] = dateutil.parser.parse(d[f[1]]).date()
            except:
                pass
        if f[0] not in r:
            r[f[0]] = None    

    if "datecollected" in r and r["datecollected"] == None:
        year = None
        month = None
        day = None        
        if "dwc:year" in d:
            try:
                year = d["dwc:year"]
                if "dwc:month" in d:
                    month = d["dwc:month"]
                    if "dwc:day" in d:                    
                        day = d["dwc:day"]
                        r["datecollected"] = dateutil.parser.parse("{0}-{1}-{2}".format(year,month,day)).date()
                    elif "dwc:startDayOfYear" in d:                    
                        day = d["dwc:startDayOfYear"]
                        r["datecollected"] = dateutil.parser.parse("{0}-{1}-{2}".format(year,month,day)).date()
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
        
    if t == "mediarecords":
        r["hasSpecimen"] = "records" in r and r["records"] != None
    elif t == "records":
        r["hasImage"] = "mediarecords" in r and r["mediarecords"] != None
    
    return r

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
    r.update(geoGrabber(t,d))            
    r.update(dateGrabber(t,d))
    r.update(relationsGrabber(t,d))
    # Done with non-dependant fields.
    r["scientificname"] = scientificNameFiller(t,r)

    r["flags"] = setFlags(r)
    r["dqs"] = score(t,r)
    for k in r.keys():
        if k.startswith("flag_"):            
            r["flags"].append("_".join(k.split("_")[1:]))
            del r[k]    
    return r



if __name__ == '__main__':
    # number Grab Testing
    # e = ["2050", "2050 m",
    #      "2050.0", "2050.0 m",
    #      "2,050", "2,050 m",
    #      "2,050.0", "2,050.0 m",
    #      "100000", "1000000",
    #      "100,000", "1,000,000",
    #      "100,000.0", "1,000,000.0"
    #     ]
    # for n in e:
    #     print n, grabFirstNumber(n), locale.atof(grabFirstNumber(n))
    
    exs = [
        # Zero Coords
        {"dwc:decimalLatitude": "0", "dwc:decimalLongitude": "0"},
        {"dwc:decimalLatitude": "1", "dwc:decimalLongitude": "0"},
        {"dwc:decimalLatitude": "0", "dwc:decimalLongitude": "1"},
        # Bounds Errors
        {"dwc:decimalLatitude": "91.00001", "dwc:decimalLongitude": "1.00001"},
        {"dwc:decimalLatitude": "-91.00001", "dwc:decimalLongitude": "1.00001"},
        {"dwc:decimalLatitude": "1.00001", "dwc:decimalLongitude": "181.00001"},
        {"dwc:decimalLatitude": "1.00001", "dwc:decimalLongitude": "-181.00001"},
        # Similar coords
        {"dwc:decimalLatitude": "1.00001", "dwc:decimalLongitude": "1.00001"},
        {"dwc:decimalLatitude": "1.00001", "dwc:decimalLongitude": "-1.00001"},
        {"dwc:decimalLatitude": "-1.00001", "dwc:decimalLongitude": "1.00001"},
        {"dwc:decimalLatitude": "-1.00001", "dwc:decimalLongitude": "-1.00001"},
        # Low Precision
        {"dwc:decimalLatitude": "30.1", "dwc:decimalLongitude": "60.2"},
        # Date Bounds
        {"dwc:eventDate": "1600-01-01"},
        {"dwc:eventDate": "2100-01-01"},
    ]

    for e in exs:
        r = grabAll("records",e)
        print r["flags"], r["dqs"], r["geopoint"], r["datecollected"]

    # test a full record
    # import requests
    # r = requests.get("http://api.idigbio.org/v1/records/d79c3b29-06b9-4a68-b13e-112a91847765")
    # ro = r.json()
    # pprint.pprint(ro)
    # d = ro["idigbio:data"]
    # d.update(ro)
    # del d["idigbio:data"]     
    # ga = grabAll("records",d)
    # pprint.pprint(ga)

    # test the first 1000 records.
    import requests
    r = requests.get("http://api.idigbio.org/v1/records?limit=1000")
    ro = r.json()
    for rec in ro["idigbio:items"]:
        recr = requests.get("http://api.idigbio.org/v1/records/" + rec["idigbio:uuid"])
        recro = recr.json()
        d = recro["idigbio:data"]
        d.update(recro)
        del d["idigbio:data"]
        ga = grabAll("records",d)
        print ga["flags"], ga["dqs"], ga["geopoint"], ga["datecollected"]