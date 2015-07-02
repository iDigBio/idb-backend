from flask import current_app, Blueprint, jsonify, abort, url_for, request

this_version = Blueprint(__name__,__name__)

import uuid

from helpers.download import generate_files
from helpers.query_shim import queryFromShim

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

    params["filename"] = str(uuid.uuid4())

    for rename in [("rq","record_query"),("mq","mediarecord_query")]:
        if params[rename[0]] is not None:
            if rename[1].endswith("query"):
                params[rename[1]] = queryFromShim(params[rename[0]])["query"]
            else:
                params[rename[1]] = params[rename[0]]
        else:
            params[rename[1]] = params[rename[0]]                
        del params[rename[0]]

    print params

    print generate_files(**params)

    return jsonify(params)

