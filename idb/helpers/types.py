"""This module provides common type annotations present in parts of the idb-backend codebase"""

import sys

if sys.version_info >= (3, 5):
    from typing import Dict, List, Tuple, Any, NewType, Optional, Union

    DwcTerm = str
    DwcTermOrQualityFlag = str
    DwcTermValue = Union[str, List[str]]
    QualityFlagValue = bool

    # consider replacing references with StrEnum for self-documentation
    if sys.version_info >= (3, 8):
        from typing import Literal
        IdbEsDocumentType = Literal['publishers', 'recordsets', 'mediarecords', 'records']
    else:
        IdbEsDocumentType = str

    # Opaque IDs
    # At time of writing, these types are frequently encountered throughout
    # idb-backend infrastructure, but knowledge of their usage has apparently
    # been lost to the original developers.
    ETag = str # a hash string; there might be different types of ETags...
    UuidStr = NewType('UuidStr', str)


    EsBulkRequestDict = Dict[str, Any]
    """'_bulk' API-formatted Elasticsearch request"""

    RecordData = Dict[DwcTermOrQualityFlag, Union[DwcTermValue, QualityFlagValue]]
else:
    # just so we avoid undefined references if any python2 code imports this
    DwcTerm = None
    DwcTermOrQualityFlag = None
    DwcTermValue = None
    QualityFlagValue = None
    IdbEsDocumentType = None
    ETag = None
    UuidStr = None
    EsBulkRequestDict = None
    RecordData = None
