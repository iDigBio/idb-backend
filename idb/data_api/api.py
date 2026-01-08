from __future__ import division, absolute_import, print_function

from flask import Flask, jsonify, request, url_for

from werkzeug.routing import BaseConverter

from idb import __version__
from idb.helpers.logging import idblogger
from idb.helpers.cors import crossdomain
from idb.postgres_backend import apidbpool
from idb.data_api.common import idbmodel

logger = idblogger.getChild("api")

app = Flask(__name__)
app.config.from_object("idb.data_api.config")
app.url_map.strict_slashes = False

# ---- register custom converters BEFORE registering blueprints ----

_UUID_RE = r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"

class NonUUIDConverter(BaseConverter):
    # Match any single segment that is NOT exactly a UUID.
    # This prevents /media/<nonuuid:etag> from stealing /media/<uuid:u>
    regex = rf"(?!{_UUID_RE}$)[^/]+"

app.url_map.converters["nonuuid"] = NonUUIDConverter

# ---- init DB / model ----

app.config["DB"] = apidbpool
idbmodel.init_app(app)

# ---- blueprints (import AFTER app + converter is ready is safest) ----

from .v1 import this_version as v1
from .v2 import this_version as v2
from .v2_download import this_version as v2_download
from .v2_media import this_version as v2_media

app.register_blueprint(v1, url_prefix="/v1")
app.register_blueprint(v2, url_prefix="/v2")
app.register_blueprint(v2_download, url_prefix="/v2")
app.register_blueprint(v2_media, url_prefix="/v2")


@app.route("/v2/", methods=["GET"])
@crossdomain(origin="*")
def v2_meta_index():
    r = {}
    for rule in app.url_map.iter_rules():
        sa = str(rule).split("/")[1:]
        if len(sa) == 2 and sa[0] == "v2":
            r[sa[1]] = url_for(rule.endpoint, _external=True)
    return jsonify(r)


@app.route("/", methods=["GET"])
@crossdomain(origin="*")
def index():
    r = {}
    for rule in app.url_map.iter_rules():
        sa = str(rule).split("/")[1:]
        if len(sa) == 2 and sa[1] == "":
            r[sa[0]] = url_for(rule.endpoint, _external=True)
    return jsonify(r)


@app.route("/version", methods=["GET"])
def version():
    return __version__


@app.route("/healthz", methods=["GET"])
def healthz():
    ver = __version__.decode("utf-8") if isinstance(__version__, (bytes, bytearray)) else str(__version__)
    return jsonify({
        "dbconn": idbmodel.fetchone("SELECT 'ok'")[0],
        "remote_addr": request.remote_addr,
        "version": ver,
    })
