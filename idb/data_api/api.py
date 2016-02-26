import os

from flask import Flask, jsonify, request, abort, url_for
from flask.ext.uuid import FlaskUUID

from idb.helpers.cors import crossdomain

from idb.postgres_backend.db import PostgresDB

app = Flask(__name__)
FlaskUUID(app)

app.config.from_object('idb.data_api.config')

app.url_map.strict_slashes = False

app.config["DB"] = PostgresDB()

from .v1 import this_version as v1
from .v2 import this_version as v2
from .v2_download import this_version as v2_download
from .v2_media import this_version as v2_media
from idb.corrections.api import this_version as corrections

app.register_blueprint(v1, url_prefix="/v1")
app.register_blueprint(v2, url_prefix="/v2")
app.register_blueprint(v2_download, url_prefix="/v2")
app.register_blueprint(v2_media, url_prefix="/v2")

if "ENV" in os.environ and os.environ["ENV"] == "test":
    app.register_blueprint(corrections, url_prefix="/v2")


@app.route('/v2/', methods=['GET'])
@crossdomain(origin="*")
def v2_meta_index():
    r = {}

    for rule in app.url_map.iter_rules():
        sa = str(rule).split("/")[1:]
        if len(sa) == 2 and sa[0] == "v2":
            r[sa[1]] = url_for(rule.endpoint, _external=True)

    return jsonify(r)


@app.route('/', methods=['GET'])
@crossdomain(origin="*")
def index():

    r = {}

    for rule in app.url_map.iter_rules():
        sa = str(rule).split("/")[1:]
        if len(sa) == 2 and sa[1] == "":
            r[sa[0]] = url_for(rule.endpoint, _external=True)

    return jsonify(r)

if __name__ == '__main__':
    app.run(debug=True)
