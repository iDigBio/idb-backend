from __future__ import division, absolute_import, print_function

from flask import current_app, Blueprint, jsonify, url_for, request

from idb.helpers.cors import crossdomain
from .common import json_error, idbmodel, logger

this_version = Blueprint(__name__,__name__)


def format_list_item(t,uuid,etag,modified,version,parent):
    links = {}
    if t in current_app.config["PARENT_MAP"] and parent is not None:
        links["".join(current_app.config["PARENT_MAP"][t][:-1])] = url_for(".item",t=current_app.config["PARENT_MAP"][t],u=parent,_external=True)
    links["".join(t[:-1])] = url_for(".item",t=t,u=uuid,_external=True)

    return {
        "idigbio:uuid": uuid,
        "idigbio:etag": etag,
        "idigbio:dateModified": modified.isoformat(),
        "idigbio:version": version,
        "idigbio:links": links,
    }


def format_item(t,uuid,etag,modified,version,parent,data,siblings,ids):
    r = format_list_item(t,uuid,etag,modified,version,parent)
    del r["idigbio:links"]["".join(t[:-1])]
    for l in r["idigbio:links"]:
        r["idigbio:links"][l] = [r["idigbio:links"][l]]
    l = {}
    if siblings is not None:
        for k in siblings:
            l[k] = []
            for i in siblings[k]:
                l[k].append(url_for(".item",t=k,u=i,_external=True))

    r["idigbio:data"] = data
    r["idigbio:links"].update(l)
    r["idigbio:recordIds"] = ids
    return r


@this_version.route('/<string:t>/<uuid:u>/<string:st>', methods=['GET','OPTIONS'])
@crossdomain(origin="*")
def subitem(t,u,st):
    if not (t in current_app.config["SUPPORTED_TYPES"] and st in current_app.config["SUPPORTED_TYPES"]):
        return json_error(404)

    limit = request.args.get("limit")
    if limit is not None:
        limit = int(limit)
    else:
        limit = 100
    offset = request.args.get("offset")
    if offset is not None:
        offset = int(offset)
    else:
        offset = 0

    r = {}
    l = [
        format_list_item(
            st,
            v["uuid"],
            v["etag"],
            v["modified"],
            v["version"],
            v["parent"],
        ) for v in idbmodel.get_children_list(str(u), "".join(st[:-1]),limit=limit,offset=offset)
    ]

    r["idigbio:items"] = l
    r["idigbio:itemCount"] = idbmodel.get_children_count(str(u), "".join(st[:-1]))
    return jsonify(r)


@this_version.route('/<string:t>/<uuid:u>', methods=['GET','OPTIONS'])
@crossdomain(origin="*")
def item(t,u):
    if t not in current_app.config["SUPPORTED_TYPES"]:
        return json_error(404)

    version = request.args.get("version")

    v = idbmodel.get_item(str(u), version=version)
    if v is not None:
        if v["data"] is None:
            return json_error(500)

        if v["type"] + "s" == t:
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
            return json_error(404)
    else:
        return json_error(404)


@this_version.route('/<string:t>', methods=['GET','OPTIONS'])
@crossdomain(origin="*")
def list(t):
    if t not in current_app.config["SUPPORTED_TYPES"]:
        return json_error(404)

    limit = request.args.get("limit")
    if limit is not None:
        limit = int(limit)
    else:
        limit = 100
    offset = request.args.get("offset")
    if offset is not None:
        offset = int(offset)
    else:
        offset = 0

    r = {}
    l = [
        format_list_item(
            t,
            v["uuid"],
            v["etag"],
            v["modified"],
            v["version"],
            v["parent"],
        ) for v in idbmodel.get_type_list("".join(t[:-1]),limit=limit,offset=offset)
    ]

    r["idigbio:items"] = l
    r["idigbio:itemCount"] = idbmodel.get_type_count("".join(t[:-1]))
    return jsonify(r)


@this_version.route('/', methods=['GET','OPTIONS'])
@crossdomain(origin="*")
def index():
    r = {}
    for t in current_app.config["SUPPORTED_TYPES"]:
        r[t] = url_for(".list",t=t,_external=True)
    return jsonify(r)
