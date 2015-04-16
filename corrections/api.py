import json
import uuid
import psycopg2

from flask import Flask, jsonify, request, abort, url_for, current_app
from flask.ext.uuid import FlaskUUID

from helpers.idb_flask_authn import requires_auth
from postgres_backend.db import PostgresDB

app = Flask(__name__)
FlaskUUID(app)

app.config["DB"] = PostgresDB()

@app.route('/', methods=['GET'])
def index():
    r = {
        "v2": url_for("v2_index",_external=True),
    }
    return jsonify(r)

@app.route('/v2', methods=['GET'])
def v2_index():
    r = {
        "annotations": url_for("get_annotations",_external=True),
        "corrections": url_for("get_corrections",_external=True),
    }
    return jsonify(r)

@app.route('/v2/annotations', methods=['GET'])
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

@app.route('/v2/annotations', methods=['POST'])
@requires_auth
def add_annotations():
    try:
        assert "uuid" in request.json
        assert "values" in request.json

        try:
            uuid.UUID(request.json["uuid"])
        except:
            resp = jsonify({"error": "Invalid UUID"})
            resp.status_code = 400
            return resp

        try:
            assert isinstance(request.json["values"],dict)
            assert len(request.json["values"])
        except:
            resp = jsonify({"error": "Values must be a non-empty dictionary"})
            resp.status_code = 400
            return resp

        try:
            cur = current_app.config["DB"].cursor()
            cur.execute("INSERT INTO annotations (uuids_id,v,source) VALUES (%s,%s,%s) RETURNING *", (request.json["uuid"],json.dumps(request.json["values"]),request.authorization.username))
            app.config["DB"].commit()
            
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
            resp = jsonify({"error": "uuid must be a valid reference to an object in idigbio"})
            resp.status_code = 400
            return resp

        except Exception as e:
            current_app.config["DB"].rollback()
            resp = jsonify({"error": repr(e)})
            resp.status_code = 500
            return resp
    except AssertionError:
        resp = jsonify({"error": "Missing required parameter from (uuid, values)"})
        resp.status_code = 400
        return resp

@app.route('/v2/annotations/<int:id>', methods=['GET'])
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

@app.route('/v2/annotations/<int:id>', methods=['PUT','POST'])
@requires_auth
def edit_annotation(id):
    #TODO
    return jsonify({})

@app.route('/v2/annotations/<int:id>/approve', methods=['PUT','POST'])
@requires_auth
def approve_annotation(id):
    #TODO
    return jsonify({})

@app.route('/v2/annotations/<int:id>/approve', methods=['DELETE'])
@requires_auth
def revoke_annotation(id):
    #TODO
    return jsonify({})

@app.route('/v2/corrections', methods=['GET'])
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

@app.route('/v2/corrections', methods=['POST'])
@requires_auth
def add_corrections():
    try:
        assert "keys" in request.json
        assert "values" in request.json

        try:
            assert isinstance(request.json["keys"],dict)
            assert len(request.json["keys"])
        except:
            resp = jsonify({"error": "Keys must be a non-empty dictionary"})
            resp.status_code = 400
            return resp

        try:
            assert isinstance(request.json["values"],dict)
            assert len(request.json["values"])
        except:
            resp = jsonify({"error": "Values must be a non-empty dictionary"})
            resp.status_code = 400
            return resp

        try:
            cur = current_app.config["DB"].cursor()
            cur.execute("INSERT INTO corrections (k,v,source) VALUES (%s,%s,%s) RETURNING *", (json.dumps(request.json["keys"]),json.dumps(request.json["values"]),request.authorization.username))
            app.config["DB"].commit()
            
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
            resp = jsonify({"error": repr(e)})
            resp.status_code = 500
            return resp
    except AssertionError:
        resp = jsonify({"error": "Missing required parameter from (keys, values)"})
        resp.status_code = 400
        return resp

@app.route('/v2/corrections/<int:id>', methods=['GET'])
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

@app.route('/v2/corrections/<int:id>', methods=['PUT','POST'])
@requires_auth
def edit_correction(id):
    #TODO
    return jsonify({})

@app.route('/v2/corrections/<int:id>/approve', methods=['PUT','POST'])
@requires_auth
def approve_correction(id):
    #TODO
    return jsonify({})

@app.route('/v2/corrections/<int:id>/approve', methods=['DELETE'])
@requires_auth
def revoke_correction(id):
    #TODO
    return jsonify({})


if __name__ == '__main__':
    app.run(debug=True)