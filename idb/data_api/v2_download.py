from __future__ import division, absolute_import, print_function

import json
import datetime

from flask import Blueprint, jsonify, url_for, request

from idigbio_workers import downloader, blocker, send_download_email, get_redis_conn

from idb.helpers.cors import crossdomain
from idb.helpers.etags import objectHasher

from .common import json_error, logger

this_version = Blueprint(__name__,__name__)

expire_time = datetime.timedelta(hours=23)


@this_version.route('/download', methods=['GET', 'POST', 'OPTIONS'])
@crossdomain(origin="*")
def download():
    redist = get_redis_conn()
    params = {
        "core_type": "records",
        "core_source": "indexterms",
        "rq": None,
        "mq": None,
        "form": "dwca-csv",
        "record_fields": None,
        "mediarecord_fields": None
    }

    if request.method == "GET":
        o = request.args
    else:
        o = request.get_json()

    if o is None:
        o = request.form

    if o is not None:
        o = dict(o)

    if "query" in o and "rq" not in o:
        o["rq"] = o["query"]

    for k in params.keys():
        if k in o:
            if isinstance(o[k],list):
                o[k] = o[k][0]

            if isinstance(o[k], str) or isinstance(o[k], unicode) and (o[k].startswith("{") or o[k].startswith("[")):
                params[k] = json.loads(o[k])
            else:
                params[k] = o[k]

    if params["rq"] is None and params["mq"] is None:
        return json_error(400, "Please supply at least one query paramter (rq,mq)")

    h = objectHasher("sha1", params, sort_arrays=True, sort_keys=True)

    email = o.get('email')
    if isinstance(email, list):
        email = email[0]
    source = o.get('source')
    if isinstance(source, list):
        source = source[0]
    force = o.get('force', False)
    forward_ip = request.access_route[0]

    rid = redist.hget(h, "id")
    if rid and not force:
        r = downloader.AsyncResult(rid)
    else:
        r = downloader.delay(params)
        redist.hmset(h, {
            "query": json.dumps(params),
            "id": r.id,
        })
        redist.set(r.id, h)
        redist.expire(r.id, expire_time.seconds - 60)
        redist.expire(h, expire_time.seconds)

    if email is not None:
        c = blocker.s(r.id) | send_download_email.s(email, params, ip=forward_ip, source=source)
        c.delay()

    dt = datetime.datetime.now() + datetime.timedelta(0, redist.ttl(h))
    if r.ready():
        params.update({
            "complete": True,
            "task_status": r.state,
            "status_url": url_for(".status", u=r.id, _external=True, _scheme='https'),
            "expires": dt.isoformat(),
            "download_url": r.get()
        })
    else:
        params.update({
            "complete": False,
            "task_status": r.state,
            "status_url": url_for(".status", u=r.id, _external=True, _scheme='https'),
            "expires": dt.isoformat()
        })

    return jsonify(params)


@this_version.route('/download/<uuid:u>', methods=['GET', 'OPTIONS'])
@crossdomain(origin="*")
def status(u):
    u = str(u)
    redist = get_redis_conn()
    try:
        r = downloader.AsyncResult(u)
        h = redist.get(u)
        if h is not None:
            params_string = redist.hget(h,"query")
            params = json.loads(params_string)
            dt = datetime.datetime.now() + datetime.timedelta(0, redist.ttl(h))
            if r.ready():
                params.update({
                    "complete": True,
                    "task_status": r.state,
                    "status_url": url_for(".status", u=r.id, _external=True, _scheme='https'),
                    "expires": dt.isoformat(),
                    "download_url": r.get()
                })
            else:
                params.update({
                    "complete": False,
                    "task_status": r.state,
                    "status_url": url_for(".status", u=r.id, _external=True, _scheme='https'),
                    "expires": dt.isoformat()
                })

            return jsonify(params)
        else:
            return json_error(404)
    except Exception:
        logger.exception("Failed getting status of download %s", u)
        params.update({
            "complete": False,
            "task_status": r.state,
            "status_url": url_for(".status", u=r.id, _external=True, _scheme='https'),
            "expires": dt.isoformat()
        })
        return jsonify(params)
