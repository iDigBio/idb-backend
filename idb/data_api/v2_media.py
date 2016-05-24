from __future__ import absolute_import
from datetime import datetime

from flask import (Blueprint, jsonify, request, redirect,
                   Response, render_template)

from idb.helpers.logging import idblogger
from idb.helpers.idb_flask_authn import requires_auth
from idb.helpers.cors import crossdomain
from idb.helpers.storage import IDigBioStorage
from idb.postgres_backend.db import MediaObject
from idb.helpers.conversions import valid_buckets, unmapped_buckets, mime_mapping
from idb.helpers.media_validation import UnknownMediaTypeError

from .common import json_error, idbmodel

this_version = Blueprint(__name__, __name__)

logger = idblogger.getChild('mediapi')

# TODO:
# List endpoints?


DERIVATIONS = {'thumbnail', 'webview', 'fullsize'}


def get_media_url(r, deriv=None):
    "Build the url for accessing the media in storage"

    if r.bucket is not None and r.etag is not None:
        if deriv is None:
            return "https://s.idigbio.org/{0}/{1}".format(
                r.bucketname, r.etag)
        elif deriv in DERIVATIONS:
            if r.derivatives:  # If derivatives have been generated
                return "https://s.idigbio.org/{0}-{2}/{1}.jpg".format(
                    r.bucketname, r.etag, deriv)
    return None


def get_json_for_record(r, deriv, **extra):
    media_url = get_media_url(r, deriv)
    d = {
        "filereference": r.url,
        "url": media_url,
        "type": r.type or r.bucket,
        "etag": r.etag,
        "modified": r.modified and r.modified.isoformat(),
        "user": r.owner,
        "mime": r.detected_mime or r.mime,
        "last_status": r.last_status
    }
    d.update(extra)
    # filter out nulls
    return {k: v for k, v in d.items() if v}


def respond_to_record(r, deriv=None, format=None):
    if r is None:
        return json_error(404)
    media_url = get_media_url(r, deriv=deriv)
    mime = r.mime or r.detected_mime
    status = 200

    if media_url:
        text = None
    elif mime is None:
        text = "Unknown Format"
    elif (r.type or r.bucket) is None:
        text = "Unsupported Format"
    elif r.last_status is None:  # haven't downloaded yet
        text = "Media Download Pending"
    else:
        text = "Media Error"
        status = 400

    if format == "json":
        d = get_json_for_record(r, deriv, text=text)
        response = jsonify(d)
        response.cache_control.public = True
        response.cache_control.max_age = 24 * 60 * 60  # 1d
        return response
    elif format is not None:
        return json_error(400, "Unknown format '{0}'".format(format))

    if media_url is not None:
        response = redirect(media_url)
        response.cache_control.public = True
        response.cache_control.max_age = 4 * 24 * 60 * 60  # 4d
        return response

    response = Response(
        render_template("_default.svg", text=text, mime=r.mime),
        mimetype="image/svg+xml")
    response.cache_control.public = True
    response.cache_control.max_age = 24 * 60 * 60  # 1d
    return response


@this_version.route('/view/mediarecords/<uuid:u>/media',
                    methods=['GET', 'OPTIONS'],
                    defaults={"format": None})
# @this_version.route('/view/records/<uuid:u>/media',
#                     methods=['GET', 'OPTIONS'],
#                     defaults={"format": None})
@this_version.route('/media/<uuid:u>',
                    methods=['GET', 'OPTIONS'],
                    defaults={"format": None})
@this_version.route('/view/mediarecords/<uuid:u>/media.<string:format>',
                    methods=['GET', 'OPTIONS'])
# @this_version.route('/view/records/<uuid:u>/media.<string:format>',
#                     methods=['GET', 'OPTIONS'])
@this_version.route('/media/<uuid:u>.<string:format>',
                    methods=['GET', 'OPTIONS'])
@crossdomain(origin="*")
def lookup_uuid(u, format):
    deriv = None
    if "deriv" in request.args:
        deriv = request.args["deriv"]
    elif "size" in request.args:
        deriv = request.args["size"]

    r = MediaObject.fromuuid(u, idbmodel=idbmodel)
    return respond_to_record(r, deriv=deriv, format=format)


@this_version.route('/media/<string:etag>',
                    methods=['GET', 'OPTIONS'],
                    defaults={"format": None})
@this_version.route('/media/<string:etag>.<string:format>',
                    methods=['GET', 'OPTIONS'])
@crossdomain(origin="*")
def lookup_etag(etag, format):
    deriv = None
    if "deriv" in request.args:
        deriv = request.args["deriv"]
    elif "size" in request.args:
        deriv = request.args["size"]

    r = MediaObject.frometag(etag, idbmodel=idbmodel)

    return respond_to_record(r, deriv=deriv, format=format)


@this_version.route('/media/',
                    methods=['GET', 'OPTIONS'],
                    defaults={"format": None})
@this_version.route('/media.<string:format>', methods=['GET', 'OPTIONS'])
@crossdomain(origin="*")
def lookup_ref(format):
    deriv = None
    if "deriv" in request.args:
        deriv = request.args["deriv"]
    elif "size" in request.args:
        deriv = request.args["size"]

    params = {}
    param_map = (("filereference", "url"), ("type", "type"),
                 ("prefix", "prefix"), ("user", "owner"), ("mime_type", "mime"))
    for ak, pk in param_map:
        if ak in request.args:
            if isinstance(request.args[ak], list):
                params[pk] = request.args[ak][0]
            else:
                params[pk] = request.args[ak]

    if "url" in params:
        mo = MediaObject.fromurl(params['url'], idbmodel=idbmodel)
        return respond_to_record(mo, deriv=deriv, format=format)

    where = []
    for k in params:
        if k == "prefix":
            where.append("media.url LIKE %(prefix)s")
            params["prefix"] += "%"
        else:
            where.append("{0}=%({0})s".format(k))
    results = MediaObject.query(conditions=where, params=params, idbmodel=idbmodel)
    logger.debug("Formatting %d results", len(results))
    files = [get_json_for_record(r, deriv) for r in results]
    return jsonify({"files": files, "count": len(files)})


@this_version.route('/media', methods=['POST'])
@crossdomain(origin="*")
@requires_auth
def upload():
    filereference = request.values.get("filereference")
    if not filereference:
        return json_error(400, "Missing filereference")

    etag = request.values.get('etag')
    obj = request.files.get('file')
    media_type = request.values.get("media_type")
    mime = request.values.get("mime")
    if media_type and media_type not in valid_buckets:
        return json_error(400, "Invalid media_type")

    if media_type not in unmapped_buckets and mime:
        # we want to validate mime type, but only if not a datasets
        mapped = mime_mapping.get(mime)
        if mapped is None or (media_type and mapped != media_type):
            return json_error(400, "Invalid mime")

    r = MediaObject.fromurl(filereference, idbmodel=idbmodel)
    if r and r.owner != request.authorization.username:
        return json_error(403)

    if obj:
        # if either type or mime are null it will be ignored, if
        # present they change the behavior of fromobj
        try:
            mo = MediaObject.fromobj(obj, type=media_type, mime=mime, url=filereference)
        except UnknownMediaTypeError:
            return json_error(400, "Invalid mime")
        mo.upload(IDigBioStorage(), obj)
        mo.insert_object(idbmodel)
    elif etag:
        mo = MediaObject.frometag(etag, idbmodel)
        if not mo or not mo.get_key(IDigBioStorage()).exists():
            return json_error(404, "Unknown etag {0!r}".format(etag))

        mo.last_status = 200
        mo.last_check = datetime.now()
        mo.mime = mime or mo.detected_mime
        mo.type = media_type or mo.bucket

    elif (media_type and mime):
        mo = r or MediaObject()
        mo.type = media_type
        mo.mime = mime
        mo.last_check = None
        mo.last_status = None
    else:
        return json_error(400, "Incomplete request")

    mo.url = filereference
    mo.owner = request.authorization.username

    if r:
        mo.update_media(idbmodel)
    else:
        mo.insert_media(idbmodel)
    if mo.etag:
        mo.ensure_media_object(idbmodel)

    idbmodel.commit()
    return respond_to_record(mo, format='json')
