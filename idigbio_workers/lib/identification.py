from __future__ import division, absolute_import, print_function
import uuid

ROOT_NAMESPACE = "http://identifiers.idigbio.org/"

locality_namespace = uuid.uuid5(uuid.NAMESPACE_URL,ROOT_NAMESPACE + "locality")
assert locality_namespace == uuid.UUID("{e8518454-3e95-5e2f-95b4-e4b25c6ebb53}")

gn_namespace = uuid.uuid5(uuid.NAMESPACE_DNS,"globalnames.org")
assert gn_namespace == uuid.UUID("{90181196-fecf-5082-a4c1-411d4f314cda}")

def identifiy_scientificname(name):
    if name is None:
        name = ""
    return str(uuid.uuid5(gn_namespace,name.encode('utf-8')))

def identifiy_locality(name):
    if name is None:
        name = ""
    return str(uuid.uuid5(locality_namespace,name.encode('utf-8')))
