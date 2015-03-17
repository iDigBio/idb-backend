from flask import Flask, jsonify, request, abort, url_for
from flask.ext.uuid import FlaskUUID

from postgres_backend.db import PostgresDB

app = Flask(__name__)
FlaskUUID(app)

app.config.from_object('config')

app.config["DB"] = PostgresDB()

from v1 import this_version as v1
from v2 import this_version as v2

app.register_blueprint(v1,url_prefix="/v1")
app.register_blueprint(v2,url_prefix="/v2")

@app.route('/', methods=['GET'])
def index():
    r = {
        "v1": url_for("v1.version_root".format(v),_external=True),
        "v2": url_for("v2.version_root".format(v),_external=True),
    }
    return jsonify(r)

if __name__ == '__main__':
    app.run(debug=True)