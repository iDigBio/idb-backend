SUPPORTED_TYPES = [
    "records",
    "mediarecords",
    "recordsets",
    "publishers"
]

PARENT_MAP = {
    "records": "recordsets",
    "mediarecords": "recordsets",
    "recordsets": "publishers",
}

SUPPORTED_VERSIONS = [
    1,
    2
]

RIAK_URL = "http://idb-riak.acis.ufl.edu:8098/buckets/{0}/keys/{1}-{2}"
