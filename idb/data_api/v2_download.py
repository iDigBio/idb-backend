from flask import current_app, Blueprint, jsonify, abort, url_for, request

this_version = Blueprint(__name__,__name__)

import uuid

from idigbio_workers import downloader

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

    downloader.delay(params)

    return jsonify(params)

