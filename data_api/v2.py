from flask import current_app, Blueprint, jsonify, abort, url_for, request

from .common import load_data_from_riak

this_version = Blueprint(__name__,__name__)

def format_list_item(t,uuid,etag,modified,version,parent):
    links = {}
    if t in current_app.config["PARENT_MAP"] and parent is not None:
        links[current_app.config["PARENT_MAP"][t]] = url_for(".item",t=current_app.config["PARENT_MAP"][t],u=parent,_external=True)
    links[t] = url_for(".item",t=t,u=uuid,_external=True)

    return {
        "uuid": uuid,
        "etag": etag,
        "modified": modified.isoformat(),
        "version": version,
        "links": links,
    }

def format_item(t,uuid,etag,modified,version,parent,data,siblings,ids):
    r = format_list_item(t,uuid,etag,modified,version,parent)
    del r["links"][t]
    for l in r["links"]:
        r["links"][l] = [r["links"][l]]

    l = {}
    if siblings is not None:
        for k in siblings:
            l[k + "s"] = []
            for i in siblings[k]:
                l[k + "s"].append(url_for(".item",t=k,u=i,_external=True))

    r["data"] = data
    r["links"].update(l)
    r["recordIds"] = ids
    return r

@this_version.route('/view/<string:t>/<uuid:u>/<string:st>/', methods=['GET'])
def subitem(t,u,st):
    if not (t in current_app.config["SUPPORTED_TYPES"] and st in current_app.config["SUPPORTED_TYPES"]):
        abort(404)

    r = {}
    l = [
        format_list_item(
            st,
            v["uuid"],
            v["etag"],
            v["modified"],
            v["version"],
            v["parent"],
        ) for v in current_app.config["DB"].get_children_list(str(u), "".join(st[:-1]))
    ]

    r["items"] = l
    r["itemCount"] = current_app.config["DB"].get_children_count(str(u), "".join(st[:-1]))
    return jsonify(r)

@this_version.route('/view/<string:t>/<uuid:u>/', methods=['GET'])
def item(t,u):
    if t not in current_app.config["SUPPORTED_TYPES"]:
        abort(404)
    
    version = request.args.get("version")

    v = current_app.config["DB"].get_item(str(u),version=version)
    if v is not None:
        if v["data"] is None:
            v["data"] = load_data_from_riak("".join(t[:-1]),u,v["riak_etag"])
        r = format_item(
            t,
            v["uuid"],
            v["etag"],
            v["modified"],
            v["version"],
            v["parent"],
            v["data"],
            v["siblings"],
            v["recordids"]
        )
        return jsonify(r)
    else:
        abort(404)

@this_version.route('/view/<string:t>/', methods=['GET'])
def list(t):
    if t not in current_app.config["SUPPORTED_TYPES"]:
        abort(404)

    r = {}
    l = [
        format_list_item(
            t,
            v["uuid"],
            v["etag"],
            v["modified"],
            v["version"],
            v["parent"],
        ) for v in current_app.config["DB"].get_type_list("".join(t[:-1]))
    ]

    r["items"] = l
    r["itemCount"] = current_app.config["DB"].get_type_count("".join(t[:-1]))
    return jsonify(r)

@this_version.route('/view/', methods=['GET'])
def view():
    r = {}
    for t in current_app.config["SUPPORTED_TYPES"]:
        r[t] = url_for(".list",t=t,_external=True)
    return jsonify(r)

@this_version.route('/', methods=['GET'])
def index():
    return jsonify({
        "view": url_for(".view",_external=True),
    })