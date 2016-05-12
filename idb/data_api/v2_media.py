from __future__ import absolute_import
from flask import (Blueprint, jsonify, url_for, request, redirect,
                   Response, render_template)

from idb.helpers.conversions import get_accessuri
from idb.helpers.idb_flask_authn import requires_auth
from idb.helpers.cors import crossdomain
from idb.helpers.storage import IDigBioStorage

from idb.postgres_backend.db import MediaObject

from .common import json_error, idbmodel

this_version = Blueprint(__name__, __name__)

# TODO:
# List endpoints?

MTYPES = {'images', 'sounds'}
DERIVATIONS = {'thumbnail', 'webview', 'fullsize'}


def get_media_url(r, deriv=None):
    (raw_media_url, media_type, objects_etag, modified, owner, derivatives,
     media_mime, last_status) = r

    if media_type is not None and objects_etag is not None:
        if deriv is None:
            return "https://s.idigbio.org/idigbio-{0}-prod/{1}".format(
                media_type, objects_etag)
        elif media_type in MTYPES and deriv in DERIVATIONS:
            if derivatives:  # If derivatives have been generated
                return "https://s.idigbio.org/idigbio-{0}-prod-{2}/{1}.jpg".format(
                    media_type, objects_etag, deriv)
    return None


def respond_to_record(r, deriv=None, format=None):
    if r is None:
        return json_error(404)

    media_url = get_media_url(r, deriv=deriv)

    (raw_media_url, media_type, objects_etag, modified, owner, derivatives,
     media_mime, last_status) = r

    if media_mime is None:
        text = "Unknown Media Format"
    elif media_type is None:
        text = "Unsupported Media Format"
    elif last_status is None:  # haven't downloaded yet
        text = "Media Download Pending"
    elif last_status == 200:
        text = None
    else:
        text = "Media Error"

    if format == "json":
        d = {
            "url": media_url,
            "etag": objects_etag,
            "filereference": raw_media_url,
            "modified": modified and modified.isoformat(),
            "user": owner,
            "text": text,
            "mime": media_mime
        }
        r = jsonify({k: v for k, v in d.items() if v})
        r.cache_control.public = True
        r.cache_control.max_age = 24 * 60 * 60  # 1d
        return r
    elif format is not None:
        return json_error(400, "Unknown format '{0}'".format(format))

    if media_url is not None:
        r = redirect(media_url)
        r.cache_control.public = True
        r.cache_control.max_age = 4 * 24 * 60 * 60  # 4d
        return r

    r = Response(
        render_template("_default.svg", text=text, mime=media_mime),
        mimetype="image/svg+xml")
    r.cache_control.public = True
    r.cache_control.max_age = 24 * 60 * 60  # 1d
    return r


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

    rec = idbmodel.get_item(str(u))
    if rec is not None:
        ref = get_accessuri(rec["type"], rec["data"])["accessuri"]
        r = idbmodel.fetchone(
            """SELECT media.url, media.type, objects.etag, modified, owner,
                      derivatives, media.mime, last_status
            FROM media
            LEFT JOIN media_objects ON media.url = media_objects.url
            LEFT JOIN objects on media_objects.etag = objects.etag
            WHERE media.url=%s
        """, (ref,))
        return respond_to_record(r, deriv=deriv, format=format)
    else:
        return json_error(404)


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

    r = idbmodel.fetchone(
        """SELECT media.url, media.type, objects.etag, modified, owner,
                  derivatives, media.mime, last_status
        FROM media
        LEFT JOIN media_objects ON media.url = media_objects.url
        LEFT JOIN objects on media_objects.etag = objects.etag
        WHERE objects.etag=%s
    """, (etag,))
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
        r = idbmodel.fetchone(
            """SELECT media.url, media.type, objects.etag, modified, owner,
                      derivatives, media.mime, last_status
            FROM media
            LEFT JOIN media_objects ON media.url = media_objects.url
            LEFT JOIN objects on media_objects.etag = objects.etag
            WHERE media.url=%(url)s
        """, params)
        return respond_to_record(r, deriv=deriv, format=format)
    else:
        where = "WHERE "
        where_a = ["objects.etag IS NOT NULL"]
        for k in params:
            if k == "prefix":
                where_a.append("media.url LIKE %({0})s".format(k))
                params[k] += "%"
            else:
                where_a.append("{0}=%({0})s".format(k))

        if len(where_a) > 0:
            where += " AND ".join(where_a)

        results = idbmodel.fetchall(
            """SELECT media.url, media.type, objects.etag, modified, owner,
                      derivatives, media.mime, last_status
            FROM media
            LEFT JOIN media_objects ON media.url = media_objects.url
            LEFT JOIN objects on media_objects.etag = objects.etag
        """ + where + " LIMIT 100", params)
        files = []
        for r in results:
            files.append({
                "filereference": r[0],
                "url": url_for(".lookup_etag",
                               etag=r[2],
                               _external=True,
                               _scheme='https',
                               deriv=deriv),
                "etag": r[2],
                "user": r[4],
                "type": r[1],
                "mime": r[6]
            })
        return jsonify({"files": files, "count": len(files)})


@this_version.route('/media', methods=['POST'])
@crossdomain(origin="*")
@requires_auth
def upload():
    try:
        filereference = request.values["filereference"]
    except KeyError:
        return json_error(400, "Missing filereference")

    media_type = request.values.get("media_type")

    r = idbmodel.fetchone(
        """SELECT url, type, owner FROM media WHERE url=%s""", (filereference,))
    if (r is not None and r["owner"] != request.authorization.username):
        return json_error(403)

    etag = request.values.get('etag', None)
    obj = request.files.get('file', None)
    if not obj and not etag:
        return json_error(400, "No file or etag posted")

    media_store = IDigBioStorage()

    if obj:
        mobject = MediaObject.fromobj(obj, mtype=media_type)
        mobject.upload(media_store, obj)
        mobject.insert_object(idbmodel)
    else:
        mobject = MediaObject.frometag(etag, idbmodel)
        if not mobject or not mobject.get_key(media_store).exists():
            return json_error(404, "Unknown etag {0!r}".format(etag))

    mobject.filereference = filereference
    mobject.owner = request.authorization.username
    if r:
        mobject.update_media(idbmodel)
    else:
        mobject.insert_media(idbmodel)

    mobject.ensure_media_object(idbmodel)

    idbmodel.commit()
    r = idbmodel.fetchone(
        """SELECT media.url, media.type, objects.etag, modified, owner,
                  derivatives, media.mime, last_status
        FROM media
        LEFT JOIN media_objects ON media.url = media_objects.url
        LEFT JOIN objects on media_objects.etag = objects.etag
        WHERE objects.etag=%s
    """, (mobject.etag,))
    return respond_to_record(r, format='json')
