import json
import uuid
import psycopg2

from flask import current_app, Blueprint, jsonify, url_for, request

from helpers.idb_flask_authn import requires_auth
from postgres_backend.db import PostgresDB
from idb.data_api.common import json_error

from corrections.record_corrector import RecordCorrector
from helpers.conversions import grabAll, index_field_to_longname
from elasticsearch_backend.indexer import prepForEs

this_version = Blueprint(__name__,__name__)

# current_app.config["DB"] = PostgresDB()

rc = RecordCorrector()

# @app.route('/', methods=['GET'])
# def index():
#     r = {
#         "v2": url_for("v2_index",_external=True),
#     }
#     return jsonify(r)

@this_version.route('/correct_record', methods=['GET','POST'])
def correct_record():
    data = None
    t = None
    if request.json is not None:
        if "data" in request.json:
            data = request.json["data"]
        if "t" in request.json:
            t = request.json["t"]
    else:
        t = request.args.get("t")
        try:
            data = json.loads(request.args.get("data"))
        except:
            return json_error(400, {"error": "Unable to parse json value in data."})

    if t is None:
        t = "records"

    if data is None:
        return json_error(400, {"error": "Must supply a value for data."})
    else:
        d,ck = rc.correct_record(data)

        g = grabAll(t,d)
        i =  prepForEs(t,g)
        dwc_rec = {}
        for k in i:
            dwc_rec[index_field_to_longname[t][k]] = i[k]
        return jsonify(dwc_rec)

# @app.route('/v2', methods=['GET'])
# def v2_index():
#     r = {
#         "annotations": url_for("get_annotations",_external=True),
#         "corrections": url_for("get_corrections",_external=True),
#     }
#     return jsonify(r)

@this_version.route('/annotations', methods=['GET'])
def get_annotations():
    limit = request.args.get("limit")
    if limit is None:
        limit = 1000
    else:
        limit = int(limit)
    offset = request.args.get("offset")
    if offset is None:
        offset = 0
    else:
        offset = int(offset)
    approved = request.args.get("approved")
    if approved is None:
        approved = True
    else:
        approved = approved == "true" or approved == "True"

    cur = current_app.config["DB"].cursor()

    cur.execute("SELECT count(*) FROM annotations WHERE approved=%s", (approved,))
    itemCount = cur.fetchone()[0]

    cur.execute("SELECT * FROM annotations WHERE approved=%s LIMIT %s OFFSET %s", (approved,limit,offset))
    l = []
    for r in cur:
        o = {
            "id": r["id"],
            "uuid": r["uuids_id"],
            "values": r["v"],
            "approved": r["approved"],
            "updated_at": r["updated_at"],
            "links": {
                "_self": url_for("view_annotation",id=r["id"],_external=True)
            }
        }
        if not r["approved"]:
            o["links"]["approve"] = url_for("approve_annotation",id=r["id"],_external=True)
        l.append(o)
    ret = {
        "items": l,
        "itemCount": itemCount,
        "links": {}
    }
    if limit + offset <= itemCount:
        ret["links"]["_next"] = url_for("get_annotations",limit=limit,offset=offset+limit,approved=approved,_external=True)
    if offset - limit >= 0:
        ret["links"]["_prev"] = url_for("get_annotations",limit=limit,offset=offset-limit,approved=approved,_external=True)
    return jsonify(ret)

@this_version.route('/annotations', methods=['POST'])
@requires_auth
def add_annotations():
    try:
        assert "uuid" in request.json
        assert "values" in request.json

        try:
            uuid.UUID(request.json["uuid"])
        except:
            return json_error(400, {"error": "Invalid UUID"})

        try:
            assert isinstance(request.json["values"],dict)
            assert len(request.json["values"])
        except:
            return json_error(400, {"error": "Values must be a non-empty dictionary"})

        try:
            cur = current_app.config["DB"].cursor()
            cur.execute("INSERT INTO annotations (uuids_id,v,source) VALUES (%s,%s,%s) RETURNING *", (request.json["uuid"],json.dumps(request.json["values"]),request.authorization.username))
            current_app.config["DB"].commit()
            
            r = cur.fetchone()
            o = {
                "id": r["id"],
                "uuid": r["uuids_id"],
                "values": r["v"],
                "approved": r["approved"],
                "updated_at": r["updated_at"],
                "links": {
                    "_self": url_for("view_annotation",id=r["id"],_external=True)
                }
            }
            if not r["approved"]:
                o["links"]["approve"] = url_for("approve_annotation",id=r["id"],_external=True)
            return jsonify(o)
        except psycopg2.IntegrityError as e:
            current_app.config["DB"].rollback()
            return json_error(400, {"error": "uuid must be a valid reference to an object in idigbio"})

        except Exception as e:
            current_app.config["DB"].rollback()
            return json_error(500, {"error": repr(e)})

    except AssertionError:
        return json_error(400, {"error": "Missing required parameter from (uuid, values)"})

@this_version.route('/annotations/<int:id>', methods=['GET'])
def view_annotation(id):
    cur = current_app.config["DB"].cursor()
    cur.execute("SELECT * FROM annotations WHERE id=%s", (id,))
    r = cur.fetchone()
    o = {
        "id": r["id"],
        "uuid": r["uuids_id"],
        "values": r["v"],
        "approved": r["approved"],
        "updated_at": r["updated_at"],
        "links": {
            "_self": url_for("view_annotation",id=r["id"],_external=True)
        }
    }
    if not r["approved"]:
        o["links"]["approve"] = url_for("approve_annotation",id=r["id"],_external=True)
    return jsonify(o)

@this_version.route('/annotations/<int:id>', methods=['PUT','POST'])
@requires_auth
def edit_annotation(id):
    #TODO
    return jsonify({})

@this_version.route('/annotations/<int:id>/approve', methods=['PUT','POST'])
@requires_auth
def approve_annotation(id):
    #TODO
    return jsonify({})

@this_version.route('/annotations/<int:id>/approve', methods=['DELETE'])
@requires_auth
def revoke_annotation(id):
    #TODO
    return jsonify({})

@this_version.route('/corrections', methods=['GET'])
def get_corrections():
    limit = request.args.get("limit")
    if limit is None:
        limit = 1000
    else:
        limit = int(limit)
    offset = request.args.get("offset")
    if offset is None:
        offset = 0
    else:
        offset = int(offset)
    approved = request.args.get("approved")
    if approved is None:
        approved = True
    else:
        approved = approved == "true" or approved == "True"

    print (limit,offset,approved)

    cur = current_app.config["DB"].cursor()

    cur.execute("SELECT count(*) FROM corrections WHERE approved=%s", (approved,))
    itemCount = cur.fetchone()[0]

    cur.execute("SELECT * FROM corrections WHERE approved=%s LIMIT %s OFFSET %s", (approved,limit,offset))
    l = []
    for r in cur:
        o = {
            "id": r["id"],
            "keys": r["k"],
            "values": r["v"],
            "approved": r["approved"],
            "updated_at": r["updated_at"],
            "links": {
                "_self": url_for("view_correction",id=r["id"],_external=True)
            }
        }
        if not r["approved"]:
            o["links"]["approve"] = url_for("approve_correction",id=r["id"],_external=True)
        l.append(o)
    ret = {
        "items": l,
        "itemCount": itemCount,
        "links": {}
    }
    if limit + offset <= itemCount:
        ret["links"]["_next"] = url_for("get_corrections",limit=limit,offset=offset+limit,approved=approved,_external=True)
    if offset - limit >= 0:
        ret["links"]["_prev"] = url_for("get_corrections",limit=limit,offset=offset-limit,approved=approved,_external=True)
    return jsonify(ret)

@this_version.route('/corrections', methods=['POST'])
@requires_auth
def add_corrections():
    try:
        assert "keys" in request.json
        assert "values" in request.json

        try:
            assert isinstance(request.json["keys"],dict)
            assert len(request.json["keys"])
        except:
            return json_error(400, {"error": "Keys must be a non-empty dictionary"})

        try:
            assert isinstance(request.json["values"],dict)
            assert len(request.json["values"])
        except:
            return json_error(400, {"error": "Values must be a non-empty dictionary"})

        try:
            cur = current_app.config["DB"].cursor()
            cur.execute("INSERT INTO corrections (k,v,source) VALUES (%s,%s,%s) RETURNING *", (json.dumps(request.json["keys"]),json.dumps(request.json["values"]),request.authorization.username))
            current_app.config["DB"].commit()
            
            r = cur.fetchone()
            o = {
                "id": r["id"],
                "keys": r["k"],
                "values": r["v"],
                "approved": r["approved"],
                "updated_at": r["updated_at"],
                "links": {
                    "_self": url_for("view_correction",id=r["id"],_external=True)
                }
            }
            if not r["approved"]:
                o["links"]["approve"] = url_for("approve_correction",id=r["id"],_external=True)
            return jsonify(o)

        except Exception as e:
            current_app.config["DB"].rollback()
            return json_error(500, {"error": repr(e)})
    except AssertionError:
        return json_error(400, {"error": "Missing required parameter from (keys, values)"})

@this_version.route('/corrections/<int:id>', methods=['GET'])
def view_correction(id):
    cur = current_app.config["DB"].cursor()
    cur.execute("SELECT * FROM corrections WHERE id=%s", (id,))
    r = cur.fetchone()
    o = {
        "id": r["id"],
        "keys": r["k"],
        "values": r["v"],
        "approved": r["approved"],
        "updated_at": r["updated_at"],
        "links": {
            "_self": url_for("view_correction",id=r["id"],_external=True)
        }
    }
    if not r["approved"]:
        o["links"]["approve"] = url_for("approve_correction",id=r["id"],_external=True)    
    return jsonify(o)

@this_version.route('/corrections/<int:id>', methods=['PUT','POST'])
@requires_auth
def edit_correction(id):
    #TODO
    return jsonify({})

@this_version.route('/corrections/<int:id>/approve', methods=['PUT','POST'])
@requires_auth
def approve_correction(id):
    #TODO
    return jsonify({})

@this_version.route('/corrections/<int:id>/approve', methods=['DELETE'])
@requires_auth
def revoke_correction(id):
    #TODO
    return jsonify({})