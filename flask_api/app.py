#!flask/bin/python
from flask import Flask, jsonify, request, abort
from flask.ext.uuid import FlaskUUID

from urllib.parse import urljoin

SUPPORTED_TYPES = [
    "records",
    "mediarecords",
    "recordsets",
    "publishers"
]

PARENT_MAP = {
    "records": "recordsets",
    "mediarecords": "recordsets",
    "recordsets": "publishers",
}

SUPPORTED_VERSIONS = [
    1,
    2
]

VIEW_URLS = {
    1: "/v1/{0}/{1}",
    2: "/v2/view/{0}/{1}"
}

db = {
    "0000012b-9bb8-42f4-ad3b-c958cb22ae45": {"type": "records", "uuid":"0000012b-9bb8-42f4-ad3b-c958cb22ae45","etag":"cb7d64ec3aef36fa4dec6a028b818e331a67aacc","modified":"2015-01-17T08:35:59.395Z","version":5,"data":{"dwc:startDayOfYear":"233","dwc:specificEpithet":"monticola","dwc:kingdom":"Plantae","dwc:recordedBy":"P. Acevedo; A. Reilly","dwc:locality":"Coral Bay Quarter, Bordeaux Mountain Road.","dwc:order":"Myrtales","dwc:habitat":"Sunny roadside.","dwc:scientificNameAuthorship":"Hitchc.","dwc:occurrenceID":"urn:uuid:ed400275-09d7-4302-b777-b4e0dcf7f2a3","id":"762944","dwc:stateProvince":"Saint John","dwc:eventDate":"1987-08-21","dwc:collectionID":"a2e32c87-d320-4a01-bafd-a9182ae2e191","dwc:country":"U.S. Virgin Islands","idigbio:recordId":"urn:uuid:ed400275-09d7-4302-b777-b4e0dcf7f2a3","dwc:collectionCode":"Plants","dwc:decimalLatitude":"18.348","dwc:occurrenceRemarks":"Small tree. 3.0 m. Bark brown, stems smooth; flowers in buds yellow.","dwc:basisOfRecord":"PreservedSpecimen","dwc:genus":"Eugenia","dwc:family":"Myrtaceae","dc:rights":"http://creativecommons.org/licenses/by-nc-sa/3.0/","dwc:identifiedBy":"Andrew Salywon, Jan 2003","dwc:dynamicProperties":"Small tree. 3.0 m. Bark brown, stems smooth; flowers in buds yellow.","symbiota:verbatimScientificName":"Eugenia monticola","dwc:phylum":"Magnoliophyta","dcterms:references":"http://swbiodiversity.org/seinet/collections/individual/index.php?occid=762944","dwc:georeferenceSources":"georef batch tool 2012-07-09","dwc:institutionCode":"ASU","dwc:reproductiveCondition":"flowers","dwc:georeferenceVerificationStatus":"reviewed - high confidence","dwc:catalogNumber":"ASU0010142","dwc:month":"8","dwc:decimalLongitude":"-64.7131","dwc:scientificName":"Eugenia monticola","dwc:otherCatalogNumbers":"156664","dwc:georeferencedBy":"jssharpe","dwc:recordNumber":"1897","dcterms:modified":"2012-07-09 12:00:09","dwc:coordinateUncertaintyInMeters":"2000","dwc:day":"21","dwc:year":"1987"},"parent": "40250f4d-7aa6-4fcc-ac38-2868fa4846bd", "siblings": {"mediarecord":["ae175cc6-82f4-456b-910c-34da322e768d","d0ca23cd-d4eb-43b5-aaba-cb75f8aef9e3"]},"recordIds":["urn:uuid:ed400275-09d7-4302-b777-b4e0dcf7f2a3"]},
    "00000140-b98f-4c9a-8628-57e10cc9241a": {"type": "records", "uuid":"00000140-b98f-4c9a-8628-57e10cc9241a","etag":"49e4955875bbb0519bee3fdfa1c48f5a70ebad17","modified":"2015-03-10T21:53:44.086Z","version":0,"data":{"dwc:startDayOfYear":"69","dwc:specificEpithet":"leucopus","dwc:countryCode":"US","dwc:kingdom":"Animalia","dwc:recordedBy":"J. P. Chapin","dwc:order":"Rodentia","dwc:individualCount":"1","dwc:occurrenceID":"urn:catalog:AMNH:Mammals:M-163002","dcterms:language":"en","dwc:occurrenceStatus":"present","dwc:establishmentMeans":"native","dwc:stateProvince":"New York","dwc:eventDate":"1906-03-10","dwc:country":"United States","dwc:collectionCode":"Mammals","id":"urn:catalog:AMNH:Mammals:M-163002","dwc:county":"Richmond","dwc:basisOfRecord":"PreservedSpecimen","dwc:genus":"Peromyscus","dwc:continent":"North America","dwc:preparations":"Skull, Cranium and Mandible, Not Mounted","dwc:sex":"female","dwc:higherClassification":"Animalia;Chordata;Mammalia;Rodentia;Muridae;Peromyscus","dwc:infraspecificEpithet":"noveboracensis","dwc:phylum":"Chordata","dwc:locality":"Staten Island, Richmond Hill","dwc:institutionID":"urn:lsid:biocol.org:col:34925","dwc:institutionCode":"AMNH","dwc:taxonRank":"subspecies","dwc:class":"Mammalia","dwc:catalogNumber":"M-163002","dwc:nomenclaturalCode":"ICZN","dcterms:type":"PhysicalObject","dwc:higherGeography":"North America;USA;New York;Richmond Co.;;;;","dwc:endDayOfYear":"69","dwc:month":"3","dwc:verbatimLocality":"North America; USA; New York; Richmond Co.; ; ; ; ; ; ; North America; USA; New York; Richmond Co.; ; ; ; ; ; ; Staten Island, Richmond Hill","dwc:verbatimEventDate":"1906-03-10","dwc:recordNumber":"49","dwc:family":"Muridae","dcterms:modified":"2013-08-07","dwc:scientificName":"Peromyscus leucopus noveboracensis","dwc:day":"10","dwc:year":"1906"},"parent": "cb790bee-26da-40ed-94e0-d179618f9bd4","recordIds":["cb790bee-26da-40ed-94e0-d179618f9bd4\\urn:catalog:amnh:mammals:m-163002"]},
    "00000230-01bc-4a4f-8389-204f39da9530": {"type": "records", "uuid":"00000230-01bc-4a4f-8389-204f39da9530","etag":"e73bdb525886ddccbbedc3952409e52d318b73e4","modified":"2014-05-27T19:15:25.542Z","version":4,"data":{"dwc:startDayOfYear":"192","dwc:specificEpithet":"luridum","dwc:county":"Keweenaw Co.","dwc:recordedBy":"F. J. Hermann","dwc:habitat":"Crevice in basalt near water level.","dwc:scientificNameAuthorship":"(Hedw.) Jenn.","dwc:occurrenceID":"1016249","dwc:stateProvince":"Michigan","dwc:eventDate":"1973-07-11","dwc:country":"United States of America","recordId":"urn:uuid:00000230-01bc-4a4f-8389-204f39da9530","dwc:rights":"http://creativecommons.org/licenses/by-nc-sa/3.0/","dwc:genus":"Hygrohypnum","dwc:family":"Campyliaceae","symbiota:verbatimScientificName":"Hygrohypnum luridum","dwc:basisOfRecord":"PreservedSpecimen","dcterms:references":"http://bryophyteportal.org/portal/collections/individual/index.php?occid=1016249","dwc:locality":"Extreme tip of Scoville Point, Rock Harbor, Isle Royale","dwc:institutionCode":"NY","dwc:catalogNumber":"00588601","dwc:month":"7","dwc:recordNumber":"25414-a","dcterms:modified":"2004-01-12 22:00:00","dwc:scientificName":"Hygrohypnum luridum","dwc:day":"11","dwc:year":"1973"},"parent":"00d9fcc1-c8e2-4ef6-be64-9994ca6a32c3","recordIds":["urn:uuid:00000230-01bc-4a4f-8389-204f39da9530"]}
}

app = Flask(__name__)
FlaskUUID(app)

def format_list_item(api_version,t,uuid,etag,modified,version,parent):
    links = {}
    if t in PARENT_MAP and parent is not None:
        links[PARENT_MAP[t]] = urlify(VIEW_URLS[api_version].format(PARENT_MAP[t],parent))
    links[t] = urlify(VIEW_URLS[api_version].format(t,uuid))

    if api_version == 2:
        return {
            "uuid": uuid,
            "etag": etag,
            "modified": modified,
            "version": version,
            "links": links,
        }
    elif api_version == 1:
        return {
            "idigbio:uuid": uuid,
            "idigbio:etag": etag,
            "idigbio:dateModified": modified,
            "idigbio:version": version,
            "idigbio:links": links,
        }

def format_item(api_version,t,uuid,etag,modified,version,parent,data,siblings):
    r = format_list_item(api_version,t,uuid,etag,modified,version,parent)
    l = {}
    if siblings is not None:
        for k in siblings:
            l[k] = []
            for i in siblings[k]:
                l[k].append(urlify(VIEW_URLS[api_version].format(k,i)))

    if api_version == 2:
        r["data"] = data
        r["links"].update(l)
    elif api_version == 1:
        r["idigbio:data"] = data
        r["idigbio:links"].update(l)
    return r

def urlify(slug):
    return urljoin(request.base_url,slug)

def get_url_version():
    for v in SUPPORTED_VERSIONS:
        if request.path.startswith("/v{0}".format(v)):
            return v

@app.route('/v2/view/<string:t>/<uuid:u>/<string:st>/', methods=['GET'])
@app.route('/v1/<string:t>/<uuid:u>/<string:st>/', methods=['GET'])
def subitem(t,u,st):
    if not (t in SUPPORTED_TYPES and st in SUPPORTED_TYPES):
        abort(404)

    api_version = get_url_version()
    r = {}
    l = [
        format_list_item(
            api_version,
            st,
            v["uuid"],
            v["etag"],
            v["modified"],
            v["version"],
            v["parent"],
        ) for k,v in db.items() if v["parent"] == str(u) and v["type"] == st
    ]
    if api_version == 2:
        r["items"] = l
        r["itemCount"] = len(l)
    elif api_version == 1:
        r["idigbio:items"] = l
        r["idigbio:itemCount"] = len(l)    
    return jsonify(r)

@app.route('/v2/view/<string:t>/<uuid:u>/', methods=['GET'])
@app.route('/v1/<string:t>/<uuid:u>/', methods=['GET'])
def item(t,u):
    if t not in SUPPORTED_TYPES:
        abort(404)

    api_version = get_url_version()
    if str(u) in db:
        v = db[str(u)]
        r = format_item(
            api_version,
            t,
            v["uuid"],
            v["etag"],
            v["modified"],
            v["version"],
            v["parent"],
            v["data"],
            v["siblings"] if "siblings" in v else None
        )
        return jsonify(r)
    else:
        abort(404)

@app.route('/v2/view/<string:t>/', methods=['GET'])
@app.route('/v1/<string:t>/', methods=['GET'])
def list(t):
    if t not in SUPPORTED_TYPES:
        abort(404)

    api_version = get_url_version()
    r = {}
    l = [
        format_list_item(
            api_version,
            t,
            v["uuid"],
            v["etag"],
            v["modified"],
            v["version"],
            v["parent"],
        ) for k,v in db.items() if v["type"] == t
    ]
    if api_version == 2:
        r["items"] = l
        r["itemCount"] = len(l)
    elif api_version == 1:
        r["idigbio:items"] = l
        r["idigbio:itemCount"] = len(l)
    return jsonify(r)

@app.route('/v2/view/', methods=['GET'])
def v2_view():
    r = {}
    for t in SUPPORTED_TYPES:
        r[t] = urlify("/v2/view/{0}/".format(t))
    return jsonify(r)

@app.route('/v2/', methods=['GET'])
def v2():
    return jsonify({
        "view": urlify("/v2/view/"),
    })

@app.route('/v1/', methods=['GET'])
def v1():
    r = {}
    for t in SUPPORTED_TYPES:
        r[t] = urlify("/v1/{0}/".format(t))
    return jsonify(r)

@app.route('/', methods=['GET'])
def index():
    r = {}
    for v in SUPPORTED_VERSIONS:
        r["v" + str(v)] = urlify("/v{0}/".format(v))
    return jsonify(r)

if __name__ == '__main__':
    app.run(debug=True)
