from flask import current_app, Blueprint, jsonify, url_for, request, redirect, Response, render_template

this_version = Blueprint(__name__,__name__)

import uuid
import redis
import datetime

from idb.helpers.etags import objectHasher, calcFileHash
from idb.helpers.conversions import get_accessuri, get_media_type
from idb.helpers.media_validation import get_validator

from idb.helpers.idb_flask_authn import requires_auth
from idb.helpers.cors import crossdomain

from .common import json_error

# TODO:
# List endpoints?
# Finish Upload


def respond_to_record(r, deriv=None, format=None):
    if r is not None:
        media_url = None
        if r[1] is not None and r[2] is not None:
            if deriv is None:
                media_url = "https://s.idigbio.org/idigbio-{0}-prod/{1}".format(r[1], r[2])
            elif r[1] == "images" and deriv in ["thumbnail", "webview", "fullsize"]:
                if r[5]: # If derivatives have been generated
                    media_url = "https://s.idigbio.org/idigbio-{0}-prod-{2}/{1}.jpg".format(r[1], r[2], deriv)

        if media_url is not None:
            if format is None:
                return redirect(media_url)
            elif format == "json":
                return jsonify({
                    "url": media_url,
                    "etag": r[2],
                    "filereference": r[0],
                    "modified": r[3].isoformat(),
                    "user": r[4]
                })
        else:
            if r[7] is None: # We haven't checked the file yet.
                return Response(render_template("_default.svg", text="Media Download Pending"), mimetype="image/svg+xml")
            if r[1] is None: # We haven't assigned the mime to a type bucket yet.
                return Response(render_template("_default.svg", text="Unsupported Media Format"), mimetype="image/svg+xml")
            elif r[6] is not None: # We haven't generated an image derivative yet.
                return Response(render_template("_default.svg", text=r[6]), mimetype="image/svg+xml")
            elif r[6] is None: # No Mime Type supplied
                return Response(render_template("_default.svg", text="Unknown Media Format"), mimetype="image/svg+xml")
    else:
        return json_error(404)

@this_version.route('/view/mediarecords/<uuid:u>/media', methods=['GET','OPTIONS'], defaults={"format": None})
@this_version.route('/view/records/<uuid:u>/media', methods=['GET','OPTIONS'], defaults={"format": None})
@this_version.route('/media/<uuid:u>', methods=['GET','OPTIONS'], defaults={"format": None})
@this_version.route('/view/mediarecords/<uuid:u>/media.<string:format>', methods=['GET','OPTIONS'])
@this_version.route('/view/records/<uuid:u>/media.<string:format>', methods=['GET','OPTIONS'])
@this_version.route('/media/<uuid:u>.<string:format>', methods=['GET','OPTIONS'])
@crossdomain(origin="*")
def lookup_uuid(u, format):
    deriv = None
    if "deriv" in request.args:
        deriv = request.args["deriv"]
    elif "size" in request.args:
        deriv = request.args["size"]

    rec = current_app.config["DB"].get_item(str(u))
    if rec is not None:
        ref = get_accessuri(rec["type"], rec["data"])["accessuri"]
        current_app.config["DB"]._cur.execute("""SELECT media.url, media.type, objects.etag, modified, owner, derivatives, media.mime, last_status
            FROM media
            LEFT JOIN media_objects ON media.url = media_objects.url
            LEFT JOIN objects on media_objects.etag = objects.etag
            WHERE media.url=%s
        """, (ref,))
        current_app.config["DB"]._pg.rollback()
        r = current_app.config["DB"]._cur.fetchone()
        print r
        return respond_to_record(r, deriv=deriv, format=format)
    else:
        return json_error(404)

@this_version.route('/media/<string:etag>', methods=['GET','OPTIONS'], defaults={"format": None})
@this_version.route('/media/<string:etag>.<string:format>', methods=['GET','OPTIONS'])
@crossdomain(origin="*")
def lookup_etag(etag, format):
    deriv = None
    if "deriv" in request.args:
        deriv = request.args["deriv"]
    elif "size" in request.args:
        deriv = request.args["size"]

    current_app.config["DB"]._cur.execute("""SELECT media.url, media.type, objects.etag, modified, owner, derivatives, media.mime
        FROM media
        LEFT JOIN media_objects ON media.url = media_objects.url
        LEFT JOIN objects on media_objects.etag = objects.etag
        WHERE objects.etag=%s
    """, (etag,))
    current_app.config["DB"]._pg.rollback()
    r = current_app.config["DB"]._cur.fetchone()
    return respond_to_record(r, deriv=deriv, format=format)

@this_version.route('/media/', methods=['GET','OPTIONS'], defaults={"format": None})
@this_version.route('/media.<string:format>', methods=['GET','OPTIONS'])
@crossdomain(origin="*")
def lookup_ref(format):
    deriv = None
    if "deriv" in request.args:
        deriv = request.args["deriv"]
    elif "size" in request.args:
        deriv = request.args["size"]

    ref = request.args["filereference"]
    current_app.config["DB"]._cur.execute("""SELECT media.url, media.type, objects.etag, modified, owner, derivatives, media.mime
        FROM media
        LEFT JOIN media_objects ON media.url = media_objects.url
        LEFT JOIN objects on media_objects.etag = objects.etag
        WHERE media.url=%s
    """, (ref,))
    current_app.config["DB"]._pg.rollback()
    r = current_app.config["DB"]._cur.fetchone()
    return respond_to_record(r, deriv=deriv, format=format)

@this_version.route('/media', methods=['POST'])
@crossdomain(origin="*")
@requires_auth
def upload():
    o = request.get_json()

    if o is None:
        o = request.form

    if o is not None:
        o = dict(o)

    filereference = o["filereference"][0]

    current_app.config["DB"]._cur.execute("""SELECT url, type, owner FROM media WHERE url=%s""", (filereference,))
    current_app.config["DB"]._pg.rollback()
    r = current_app.config["DB"]._cur.fetchone()

    if r is None or r[2] == request.authorization.username:
        mime = "image/jpeg"
        if "mime_type" in o:
            mime = o["mime_type"][0]

        mt = get_media_type("mediarecords",{"dc:format": mime })

        file = request.files["file"]

        h, size = calcFileHash(request.files["file"],op=False,return_size=True)

        validator = get_validator(mime)

        request.files["file"].seek(0)

        valid, detected_mime = validator(file.filename,mt["type"],mime,request.files["file"].read(1024))

        if valid:
            return jsonify({
                "file_size": size,
                "file_name": unicode(file.filename),
                "file_md5": h,
                "object_type": mt["type"],
                "file_reference": filereference,
                "file_url": url_for(".lookup_etag", etag=h, _external=True, _scheme='https'),
                "content_type": detected_mime
            })
        else:
            return json_error(400,"Invalid Content Type")
    else:
        return json_error(403)