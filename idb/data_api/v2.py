from __future__ import division, absolute_import, print_function

from flask import current_app, Blueprint, jsonify, url_for, request

from idb.helpers.cors import crossdomain
from idb.helpers.storage import IDigBioStorage
from .common import json_error, idbmodel


this_version = Blueprint(__name__.replace(".", "-"), __name__.replace(".", "-"))

# Constrain type-like path segments so they can't accidentally match UUIDs.
# This prevents /view/<string:t> from stealing /view/<uuid:u>.
SUPPORTED = "records,mediarecords,recordsets,publishers"


def format_list_item(t, uuid, etag, modified, version, parent):
    links = {}
    if t in current_app.config["PARENT_MAP"] and parent is not None:
        links[current_app.config["PARENT_MAP"][t]] = url_for(
            ".item",
            t=current_app.config["PARENT_MAP"][t],
            u=parent,
            _external=True,
        )
    links[t] = url_for(".item", t=t, u=uuid, _external=True)

    return {
        "type": t,
        "uuid": uuid,
        "etag": etag,
        "modified": modified.isoformat(),
        "version": version,
        "links": links,
    }


def format_item(t, uuid, etag, modified, version, parent, data, siblings, ids):
    r = format_list_item(t, uuid, etag, modified, version, parent)
    del r["links"][t]
    for l in r["links"]:
        r["links"][l] = [r["links"][l]]

    l = {}
    if siblings is not None:
        for k in siblings:
            l[k + "s"] = []
            for i in siblings[k]:
                l[k + "s"].append(url_for(".item", t=k, u=i, _external=True))

    r["type"] = t
    r["data"] = data
    r["links"].update(l)
    r["recordIds"] = ids
    return r


@this_version.route(f"/view/<any({SUPPORTED}):t>/<uuid:u>/<any({SUPPORTED}):st>",
                    methods=["GET", "OPTIONS"])
@crossdomain(origin="*")
def subitem(t, u, st):
    # This check is now redundant because the route only matches supported types,
    # but keeping it doesn't hurt if you want defense-in-depth.
    if not (t in current_app.config["SUPPORTED_TYPES"] and
            st in current_app.config["SUPPORTED_TYPES"]):
        return json_error(404)

    limit = request.args.get("limit")
    limit = int(limit) if limit is not None else 100

    offset = request.args.get("offset")
    offset = int(offset) if offset is not None else 0

    r = {}
    l = idbmodel.get_children_list(str(u), "".join(st[:-1]), limit=limit, offset=offset)

    r["items"] = [
        format_list_item(
            st,
            v["uuid"],
            v["etag"],
            v["modified"],
            v["version"],
            v["parent"],
        )
        for v in l
    ]
    r["itemCount"] = idbmodel.get_children_count(str(u), "".join(st[:-1]))
    return jsonify(r)


@this_version.route("/file/<uuid:u>", methods=["GET", "OPTIONS"])
@crossdomain(origin="*")
def file(u):
    version = request.args.get("version")  # left as-is; not used in this snippet
    fname = str(u)
    f = idbmodel.fetch_file(str(u), fname, media_store=IDigBioStorage())
    return f


@this_version.route(f"/view/<any({SUPPORTED}):t>/<uuid:u>", methods=["GET", "OPTIONS"])
@crossdomain(origin="*")
def item(t, u):
    if t not in current_app.config["SUPPORTED_TYPES"]:
        return json_error(404)

    version = request.args.get("version")

    v = idbmodel.get_item(str(u), version=version)
    if v is None:
        return json_error(404)
    if v["data"] is None:
        return json_error(500)

    if v["type"] + "s" != t:
        return json_error(404)

    r = format_item(
        t, v["uuid"], v["etag"], v["modified"],
        v["version"], v["parent"], v["data"],
        v["siblings"], v["recordids"],
    )
    return jsonify(r)


@this_version.route("/view/<uuid:u>", methods=["GET", "OPTIONS"])
@crossdomain(origin="*")
def item_no_type(u):
    version = request.args.get("version")

    v = idbmodel.get_item(str(u), version=version)
    if v is None:
        return json_error(404)
    if v["data"] is None:
        return json_error(500)

    r = format_item(
        v["type"] + "s", v["uuid"], v["etag"], v["modified"],
        v["version"], v["parent"], v["data"], v["siblings"],
        v["recordids"],
    )
    return jsonify(r)


@this_version.route(f"/view/<any({SUPPORTED}):t>", methods=["GET", "OPTIONS"])
@crossdomain(origin="*")
def list(t):
    # This check is now redundant because the route only matches supported types.
    if t not in current_app.config["SUPPORTED_TYPES"]:
        return json_error(404)

    limit = request.args.get("limit")
    limit = int(limit) if limit is not None else 100

    offset = request.args.get("offset")
    offset = int(offset) if offset is not None else 0

    l = [
        format_list_item(
            t,
            v["uuid"],
            v["etag"],
            v["modified"],
            v["version"],
            v["parent"],
        )
        for v in idbmodel.get_type_list("".join(t[:-1]), limit=limit, offset=offset)
    ]

    r = {
        "items": l,
        "itemCount": idbmodel.get_type_count("".join(t[:-1])),
    }
    return jsonify(r)


@this_version.route("/view", methods=["GET", "OPTIONS"])
@crossdomain(origin="*")
def view():
    r = {}
    for t in current_app.config["SUPPORTED_TYPES"]:
        r[t] = url_for(".list", t=t, _external=True)
    return jsonify(r)
