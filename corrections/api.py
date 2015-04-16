from flask import Flask, jsonify, request, abort, url_for
from flask.ext.uuid import FlaskUUID

from postgres_backend.db import PostgresDB

app = Flask(__name__)
FlaskUUID(app)

app.config["DB"] = PostgresDB()

@app.route('/', methods=['GET'])
def index():
    r = {
        "v2": url_for("v2_index",_external=True),
    }
    return jsonify(r)

@app.route('/v2', methods=['GET'])
def v2_index():
    r = {
        "annotations": url_for("get_annotations",_external=True),
        "corrections": url_for("get_corrections",_external=True),
    }
    return jsonify(r)

@app.route('/v2/annotations', methods=['GET'])
def get_annotations():
    return jsonify({})

@app.route('/v2/annotations', methods=['POST'])
def add_annotations():
    return jsonify({})

@app.route('/v2/annotations/<int:id>', methods=['GET'])
def view_annotation():
    return jsonify({})

@app.route('/v2/annotations/<int:id>', methods=['PUT','POST'])
def edit_annotation():
    return jsonify({})

@app.route('/v2/annotations/<int:id>/approve', methods=['PUT','POST'])
def approve_annotation():
    return jsonify({})

@app.route('/v2/corrections', methods=['GET'])
def get_corrections():
    return jsonify({})

@app.route('/v2/corrections', methods=['POST'])
def add_corrections():
    return jsonify({})

@app.route('/v2/corrections/<int:id>', methods=['GET'])
def view_correction():
    return jsonify({})

@app.route('/v2/corrections/<int:id>', methods=['PUT','POST'])
def edit_correction():
    return jsonify({})

@app.route('/v2/corrections/<int:id>/approve', methods=['PUT','POST'])
def approve_correction():
    return jsonify({})


if __name__ == '__main__':
    app.run(debug=True)