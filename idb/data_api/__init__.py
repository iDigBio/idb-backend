from flask import Flask, jsonify, request, abort, url_for
from flask.ext.uuid import FlaskUUID

import sys, os
sys.path.append(os.path.abspath(os.path.dirname(os.path.dirname(__file__))))

from postgres_backend.db import PostgresDB

app = Flask(__name__)
FlaskUUID(app)

app.config.from_object('data_api.config')

app.config["DB"] = PostgresDB()

from v1 import this_version as v1
from v2 import this_version as v2

app.register_blueprint(v1,url_prefix="/v1")
app.register_blueprint(v2,url_prefix="/v2")

@app.route('/', methods=['GET'])
def index():
    r = {
        "v1": url_for("data_api.v1.index",_external=True),
        "v2": url_for("data_api.v2.index",_external=True),
    }
    return jsonify(r)

if __name__ == '__main__':
    app.run(debug=True)