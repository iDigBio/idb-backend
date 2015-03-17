from flask import current_app, Blueprint, jsonify, abort, url_for

this_version = Blueprint(__name__,__name__)

def format_list_item(t,uuid,etag,modified,version,parent):
    links = {}
    if t in current_app.config["PARENT_MAP"] and parent is not None:
        links[current_app.config["PARENT_MAP"][t]] = url_for(".item",t=current_app.config["PARENT_MAP"][t],u=parent,_external=True)
    links[t] = url_for(".item",t=t,u=uuid,_external=True)

    return {
        "uuid": uuid,
        "etag": etag,
        "modified": modified,
        "version": version,
        "links": links,
    }

def format_item(t,uuid,etag,modified,version,parent,data,siblings):
    r = format_list_item(t,uuid,etag,modified,version,parent)
    l = {}
    if siblings is not None:
        for k in siblings:
            l[k] = []
            for i in siblings[k]:
                l[k].append(url_for(".item",t=k,u=i,_external=True))

    r["data"] = data
    r["links"].update(l)
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
        ) for k,v in current_app.config["DB"].items() if v["parent"] == str(u) and v["type"] == st
    ]

    r["items"] = l
    r["itemCount"] = len(l)   
    return jsonify(r)

@this_version.route('/view/<string:t>/<uuid:u>/', methods=['GET'])
def item(t,u):
    if t not in current_app.config["SUPPORTED_TYPES"]:
        abort(404)
    
    if str(u) in current_app.config["DB"]:
        v = current_app.config["DB"][str(u)]
        r = format_item(
            t,
            v["uuid"],
            v["etag"],
            v["modified"],
            v["version"],
            v["parent"],
            v["data"],
            v["siblings"] if "siblings" in v else None
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
        ) for k,v in current_app.config["DB"].items() if v["type"] == t
    ]

    r["items"] = l
    r["itemCount"] = len(l)
    return jsonify(r)

@this_version.route('/view/', methods=['GET'])
def view():
    r = {}
    for t in current_app.config["SUPPORTED_TYPES"]:
        r[t] = url_for(".list",t=t,_external=True)
    return jsonify(r)

@this_version.route('/', methods=['GET'])
def version_root():
    return jsonify({
        "view": url_for(".view",_external=True),
    })