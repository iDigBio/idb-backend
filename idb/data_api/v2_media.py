from flask import (current_app, Blueprint, jsonify, url_for, request, redirect,
                   Response, render_template)

from idb.helpers.etags import objectHasher, calcFileHash
from idb.helpers.conversions import get_accessuri, get_media_type
from idb.helpers.media_validation import get_validator
from idb.helpers.idb_flask_authn import requires_auth
from idb.helpers.cors import crossdomain
from idb.helpers.storage import IDigBioStorage

from .common import json_error

this_version = Blueprint(__name__, __name__)

# TODO:
# List endpoints?

media_store = IDigBioStorage()

MTYPES = {'images', 'sounds'}
DERIVATIONS = {'thumbnail', 'webview', 'fullsize'}


def get_media_url(r, deriv=None):
    (raw_media_url, media_type, objects_etag, modified, owner, derivatives,
     media_mime, last_status) = r

    if media_type is not None and objects_etag is not None:
        if deriv is None:
            return "https://s.idigbio.org/idigbio-{0}-prod/{1}".format(
                media_type, objects_etag)
        elif media_type in MTYPES and deriv in DERIV:
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

    if media_url is not None:
        if format is None:
            return redirect(media_url)
        elif format == "json":
            return jsonify({
                "url": media_url,
                "etag": objects_etag,
                "filereference": raw_media_url,
                "modified": modified.isoformat(),
                "user": owner
            })
    else:
        if media_type is None:  # We haven't assigned the mime to a type bucket yet.
            return Response(
                render_template("_default.svg",
                                text="Unsupported Media Format"),
                mimetype="image/svg+xml")
        elif last_status is None:  # We haven't checked the file yet.
            return Response(
                render_template("_default.svg",
                                text="Media Download Pending"),
                mimetype="image/svg+xml")
        elif media_mime is not None:  # We haven't generated an image derivative yet.
            return Response(
                render_template("_default.svg",
                                text=media_mime),
                mimetype="image/svg+xml")
        elif media_mime is None:  # No Mime Type supplied
            return Response(
                render_template("_default.svg",
                                text="Unknown Media Format"),
                mimetype="image/svg+xml")


@this_version.route('/view/mediarecords/<uuid:u>/media',
                    methods=['GET', 'OPTIONS'],
                    defaults={"format": None})
@this_version.route('/view/records/<uuid:u>/media',
                    methods=['GET', 'OPTIONS'],
                    defaults={"format": None})
@this_version.route('/media/<uuid:u>',
                    methods=['GET', 'OPTIONS'],
                    defaults={"format": None})
@this_version.route('/view/mediarecords/<uuid:u>/media.<string:format>',
                    methods=['GET', 'OPTIONS'])
@this_version.route('/view/records/<uuid:u>/media.<string:format>',
                    methods=['GET', 'OPTIONS'])
@this_version.route('/media/<uuid:u>.<string:format>',
                    methods=['GET', 'OPTIONS'])
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
        current_app.config["DB"]._cur.execute(
            """SELECT media.url, media.type, objects.etag, modified, owner,
                      derivatives, media.mime, last_status
            FROM media
            LEFT JOIN media_objects ON media.url = media_objects.url
            LEFT JOIN objects on media_objects.etag = objects.etag
            WHERE media.url=%s
        """, (ref,))
        current_app.config["DB"]._pg.rollback()
        r = current_app.config["DB"]._cur.fetchone()
        #print r
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

    current_app.config["DB"]._cur.execute(
        """SELECT media.url, media.type, objects.etag, modified, owner,
                  derivatives, media.mime, last_status
        FROM media
        LEFT JOIN media_objects ON media.url = media_objects.url
        LEFT JOIN objects on media_objects.etag = objects.etag
        WHERE objects.etag=%s
    """, (etag,))
    current_app.config["DB"]._pg.rollback()
    r = current_app.config["DB"]._cur.fetchone()
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
        current_app.config["DB"]._cur.execute(
            """SELECT media.url, media.type, objects.etag, modified, owner,
                      derivatives, media.mime, last_status
            FROM media
            LEFT JOIN media_objects ON media.url = media_objects.url
            LEFT JOIN objects on media_objects.etag = objects.etag
            WHERE media.url=%(url)s
        """, params)
        current_app.config["DB"]._pg.rollback()
        r = current_app.config["DB"]._cur.fetchone()
        #print r
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

        current_app.config["DB"]._cur.execute(
            """SELECT media.url, media.type, objects.etag, modified, owner,
                      derivatives, media.mime, last_status
            FROM media
            LEFT JOIN media_objects ON media.url = media_objects.url
            LEFT JOIN objects on media_objects.etag = objects.etag
        """ + where + " LIMIT 100", params)
        print current_app.config["DB"]._cur.query
        current_app.config["DB"]._pg.rollback()

        files = []
        for r in current_app.config["DB"]._cur:
            print r
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
    o = request.get_json()
    if o is None:
        o = request.form
    if o is not None:
        o = dict(o)

    filereference = o["filereference"][0]

    current_app.config["DB"]._cur.execute(
        """SELECT url, type, owner FROM media WHERE url=%s""", (filereference,))
    current_app.config["DB"]._pg.rollback()
    r = current_app.config["DB"]._cur.fetchone()

    if (r is not None and r[2] != request.authorization.username):
        return json_error(403)

    mime = "image/jpeg"
    if "mime_type" in o:
        mime = o["mime_type"][0]

    mt = get_media_type("mediarecords", {"dc:format": mime})

    file = request.files["file"]

    h, size = calcFileHash(request.files["file"], op=False, return_size=True)

    validator = get_validator(mime)

    request.files["file"].seek(0)

    valid, detected_mime = validator(file.filename, mt["mediatype"], mime,
                                     request.files["file"].read(1024))

    if not valid or mt["mediatype"] is None:
        return json_error(400, "Invalid Content Type")

    request.files["file"].seek(0)
    k = media_store.get_key(h, "idigbio-{0}-{1}".format(mt["mediatype"],
                                                        "prod"))
    k.set_contents_from_file(request.files["file"],
                             md5=k.get_md5_from_hexdigest(h))
    k.make_public()
    if r is None:
        current_app.config["DB"]._cur.execute(
            "INSERT INTO media (url,type,mime,last_status,last_check,owner) VALUES (%s,%s,%s,200,now(),%s)",
            (filereference, mt["mediatype"], mime,
             request.authorization.username))
    else:
        current_app.config["DB"]._cur.execute(
            "UPDATE media SET last_check=now(), last_status=200, type=%s, mime=%s WHERE url=%s",
            (mt["mediatype"], mime, filereference))
    current_app.config["DB"]._cur.execute(
        "INSERT INTO objects (bucket, etag, detected_mime) (SELECT %s, %s, %s WHERE NOT EXISTS (SELECT 1 FROM objects WHERE etag=%s))",
        (mt["mediatype"], h, detected_mime, h))
    current_app.config["DB"]._cur.execute(
        "INSERT INTO media_objects (url, etag) VALUES (%s,%s)", (
            filereference, h))
    current_app.config["DB"]._pg.commit()
    return jsonify({
        "file_size": size,
        "file_name": unicode(file.filename),
        "file_md5": h,
        "object_type": mt["mediatype"],
        "file_reference": filereference,
        "file_url": url_for(".lookup_etag",
                            etag=h,
                            _external=True,
                            _scheme='https'),
        "content_type": detected_mime
    })
