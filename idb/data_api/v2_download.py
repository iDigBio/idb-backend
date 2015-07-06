from flask import current_app, Blueprint, jsonify, abort, url_for, request

this_version = Blueprint(__name__,__name__)

import uuid
import redis
import json
import datetime

from idigbio_workers import downloader, send_download_email

from helpers.etags import objectHasher

expire_time_in_seconds = 23 * 60 * 60

redist = redis.StrictRedis(host='idb-redis-celery.acis.ufl.edu', port=6379, db=0)

@this_version.route('/download', methods=['GET','POST'])
def download():

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

    for k in params.keys():
        if k in o:
            params[k] = o[k]

    h = objectHasher("sha1",params,sort_arrays=True,sort_keys=True)

    email = None
    source = None
    force = False
    forward_ip = request.access_route[0]

    if "email" in o:
        email = o["email"]

    if "source" in o:
        source = o["source"]

    if "force" in o:
        force = True

    dispatch = False
    if redist.exists(h) and not force:
        r = downloader.AsyncResult(redist.hget(h,"id"))
        if r.ready() and email is not None:
            send_download_email(email,r.get(),params,ip=forward_ip,source=source)
    else:
        r = downloader.delay(params,email=email,source=source,ip=forward_ip)
        dispatch = True

    if dispatch:
        redist.hmset(h, {
            "query": json.dumps(params),
            "id": r.id,
            "email": email
        })
        redist.set(r.id,h)
        redist.expire(r.id,expire_time_in_seconds - 60)
        redist.expire(h,expire_time_in_seconds)
    elif r.ready() and email is not None:
        send_download_email(email,r.get(),params,ip=forward_ip,source=source)

    dt = datetime.datetime.now() + datetime.timedelta(0,redist.ttl(h))
    if r.ready():
        params.update({
            "complete": True,
            "task_status": r.state,
            "status_url": url_for(".status",u=r.id,_external=True),
            "expires": dt.isoformat(),
            "download_url": r.get()
        })
    else:
        params.update({
            "complete": False,
            "task_status": r.state,
            "status_url": url_for(".status",u=r.id,_external=True),
            "expires": dt.isoformat()
        })

    return jsonify(params)

@this_version.route('/download/<uuid:u>', methods=['GET'])
def status(u):
    u = str(u)
    r = downloader.AsyncResult(u)
    h = redist.get(u)
    if h is not None:
        params_string = redist.hget(h,"query")
        params = json.loads(params_string)
        dt = datetime.datetime.now() + datetime.timedelta(0,redist.ttl(h))
        if r.ready():
            params.update({
                "complete": True,
                "task_status": r.state,
                "status_url": url_for(".status",u=r.id,_external=True),
                "expires": dt.isoformat(),
                "download_url": r.get()
            })
        else:
            params.update({
                "complete": False,
                "task_status": r.state,
                "status_url": url_for(".status",u=r.id,_external=True),
                "expires": dt.isoformat()
            })

        return jsonify(params)
    else:
        abort(404)
