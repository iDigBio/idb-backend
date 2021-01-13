from __future__ import division, absolute_import, print_function

import json
import gevent
import udatetime
from datetime import timedelta


from flask import Blueprint, jsonify, url_for, request, redirect

from idigbio_workers import downloader, blocker, send_download_email, get_redis_conn

from idb.helpers.cors import crossdomain
from idb.helpers.etags import objectHasher

from .common import json_error, logger

this_version = Blueprint(__name__,__name__)

QUERY_VALID_TIME = timedelta(hours=23)
TASK_EXPIRE_TIME = timedelta(days=30)
DOWNLOADER_TASK_PREFIX = "downloader:"


@this_version.route('/download', methods=['GET', 'POST', 'OPTIONS'])
@crossdomain(origin="*")
def download():
    rconn = get_redis_conn()
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

            if isinstance(o[k], str):
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
    forward_ip = request.remote_addr
    query_hash = objectHasher("sha1", params, sort_arrays=True, sort_keys=True)
    rqhk = DOWNLOADER_TASK_PREFIX + query_hash  # redis query hash key

    tid = None
    if not force:
        tid = rconn.get(rqhk)
        if tid:
            tdata = get_task_status(tid)
            if not tdata or tdata.get('task_status') in ('FAILURE', 'UNKNOWN'):
                tid = None

    if not tid:
        tid = downloader.delay(params).id
        rtkey = DOWNLOADER_TASK_PREFIX + tid
        rconn.hmset(rtkey, {
            "query": json.dumps(params),
            "hash": query_hash,
            "created": udatetime.utcnow_to_string()
        })
        rconn.expire(rtkey, TASK_EXPIRE_TIME)
        rconn.set(rqhk, tid)
        rconn.expire(rqhk, QUERY_VALID_TIME)
        logger.debug("Started task: %s", tid)

    if email is not None:
        c = blocker.s(tid) | send_download_email.s(email, params, ip=forward_ip, source=source)
        c.delay()

    return status(tid)


def get_task_status(tid):
    rconn = get_redis_conn()
    rtkey = DOWNLOADER_TASK_PREFIX + tid
    try:
        tdata = rconn.hgetall(rtkey)
        if len(tdata) == 0:
            return None
        tdata["query"] = json.loads(tdata["query"])
        tdata["status_url"] = url_for(".status", tid=tid, _external=True)
        ttl = rconn.ttl(rtkey)
        if ttl != -1:
            ttl = timedelta(seconds=ttl)
            tdata["expires"] = udatetime.to_string(udatetime.utcnow() + ttl)
        if tdata.get('task_status'):
            # `task_status` isn't set in redis while pending, so if
            # set then it is complete
            tdata["complete"] = True
        else:
            ar = downloader.AsyncResult(tid)
            tdata['task_status'] = ar.status
            tdata["complete"] = ar.ready()
            if ar.ready():
                rconn.hset(rtkey, "task_status", ar.status)
                if ar.successful():
                    tdata["download_url"] = ar.result
                    rconn.hset(rtkey, "download_url", ar.result)
                elif ar.failed():
                    tdata["error"] = str(ar.result)
                    rconn.hset(rtkey, "error", str(ar.result))
                    gevent.spawn(dissociate_query_hash, tid, tdata)
    except Exception as e:
        logger.exception("Failed getting status of download %s", tid)
        tdata = {
            "complete": False,
            "task_status": "UNKNOWN",
            "status_url": url_for(".status", tid=tid, _external=True),
            "error": str(e)
        }
    return tdata


@this_version.route('/download/<uuid:tid>', methods=['GET', 'OPTIONS'])
@crossdomain(origin="*")
def status(tid):
    tdata = get_task_status(str(tid))
    if tdata is None:
        return json_error(404)
    return jsonify(tdata)


def dissociate_query_hash(tid, task_data):
    if not task_data.get('hash'):
        return
    rconn = get_redis_conn()
    rqhk = DOWNLOADER_TASK_PREFIX + task_data['hash']  # redis query hash key
    if rconn.get(rqhk) == tid:
        rconn.delete(rqhk)
