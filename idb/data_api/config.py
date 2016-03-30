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
