from flask import current_app, Blueprint, jsonify, abort, url_for

this_version = Blueprint(__name__,__name__)

def format_list_item(t,uuid,etag,modified,version,parent):
    links = {}
    if t in current_app.config["PARENT_MAP"] and parent is not None:
        links[current_app.config["PARENT_MAP"][t]] = url_for(".item",t=current_app.config["PARENT_MAP"][t],u=parent,_external=True)
    links[t] = url_for(".item",t=t,u=uuid,_external=True)

    return {
        "idigbio:uuid": uuid,
        "idigbio:etag": etag,
        "idigbio:dateModified": modified.isoformat(),
        "idigbio:version": version,
        "idigbio:links": links,
    }

def format_item(t,uuid,etag,modified,version,parent,data,siblings):
    r = format_list_item(t,uuid,etag,modified,version,parent)
    l = {}
    if siblings is not None:
        for k in siblings:
            l[k] = []
            for i in siblings[k]:
                l[k].append(url_for(".item",t=k,u=i,_external=True))

    r["idigbio:data"] = data
    r["idigbio:links"].update(l)
    return r

@this_version.route('/<string:t>/<uuid:u>/<string:st>/', methods=['GET'])
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

    r["idigbio:items"] = l
    r["idigbio:itemCount"] = len(l)    
    return jsonify(r)


@this_version.route('/<string:t>/<uuid:u>/', methods=['GET'])
def item(t,u):
    if t not in current_app.config["SUPPORTED_TYPES"]:
        abort(404)

    v = current_app.config["DB"].get_item(str(u))
    if v is not None:
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


@this_version.route('/<string:t>/', methods=['GET'])
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

    r["idigbio:items"] = l
    r["idigbio:itemCount"] = len(l)
    return jsonify(r)

@this_version.route('/', methods=['GET'])
def index():
    r = {}
    for t in current_app.config["SUPPORTED_TYPES"]:
        r[t] = url_for(".list",t=t,_external=True)
    return jsonify(r)