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

VIEW_URLS = {
    1: "/v1/{0}/{1}",
    2: "/v2/view/{0}/{1}"
}