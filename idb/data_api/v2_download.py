from __future__ import division, absolute_import, print_function

import json
from datetime import datetime, timedelta


from flask import Blueprint, jsonify, url_for, request, redirect

from idigbio_workers import downloader, blocker, send_download_email, get_redis_conn

from idb.helpers.cors import crossdomain
from idb.helpers.etags import objectHasher

from .common import json_error, logger

this_version = Blueprint(__name__,__name__)

QUERY_VALID_TIME = timedelta(hours=23)

DOWNLOADER_TASK_PREFIX = "downloader:"


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
            if isinstance(o[k], list):
                o[k] = o[k][0]

            if isinstance(o[k], basestring):
                try:
                    params[k] = json.loads(o[k])
                except ValueError:
                    params[k] = o[k]

    if params["rq"] is None and params["mq"] is None:
        return json_error(400, "Please supply at least one query paramter (rq,mq)")

    email = o.get('email')
    if isinstance(email, list):
        email = email[0]
    source = o.get('source')
    if isinstance(source, list):
        source = source[0]
    force = o.get('force', False)
    forward_ip = request.access_route[0]

    query_hash = objectHasher("sha1", params, sort_arrays=True, sort_keys=True)
    rqhk = DOWNLOADER_TASK_PREFIX + query_hash  # redis query hash key
    tid = redist.get(rqhk)
    ar = None

    if force or not tid or downloader.AsyncResult(tid).failed():
        ar = downloader.delay(params)
        tid = ar.id
        redist.hmset(DOWNLOADER_TASK_PREFIX + tid, {
            "query": json.dumps(params),
            "hash": query_hash,
            "created": datetime.now()
        })
        redist.set(rqhk, tid)
        redist.expire(rqhk, QUERY_VALID_TIME)
        logger.debug("Started task: %s", tid)

    if email is not None:
        c = blocker.s(tid) | send_download_email.s(email, params, ip=forward_ip, source=source)
        c.delay()

    return redirect(url_for(".status", tid=tid, _external=True), code=302)




@this_version.route('/download/<uuid:tid>', methods=['GET', 'OPTIONS'])
@crossdomain(origin="*")
def status(tid):
    tid = str(tid)
    redist = get_redis_conn()
    redis_task_key = DOWNLOADER_TASK_PREFIX + tid
    try:
        query, created, link, qhash = redist.hmget(redis_task_key, ("query", "created", "link", "hash"))
        if query is None:
            return json_error(404)

        params = json.loads(query)
        params["status_url"] = url_for(".status", tid=tid, _external=True)
        params["expires"] = datetime.now() + timedelta(seconds=redist.ttl(tid))
        params["created"] = created
        if link:
            params["task_status"] = "SUCCESS"
            params["complete"] = True
            params["download_url"] = link
        else:
            ar = downloader.AsyncResult(tid)
            params["complete"] = ar.ready()
            params["task_status"] = ar.state

            if ar.successful():
                params["download_url"] = ar.get()
                redist.hset(redis_task_key, "link", params["download_url"])
            elif ar.failed():
                rqhk = DOWNLOADER_TASK_PREFIX + qhash  # redis query hash key
                params["error"] = str(ar.info)
                if redist.get(rqhk) == tid:
                    redist.delete(rqhk)
        return jsonify(params)

    except Exception as e:
        logger.exception("Failed getting status of download %s", tid)
        return jsonify({
            "complete": False,
            "task_status": "UNKNOWN",
            "status_url": url_for(".status", tid=tid, _external=True),
            "error": str(e)
        })
