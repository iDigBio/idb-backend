from __future__ import division, absolute_import, print_function
from enum import Enum


IGNORE_PREFIXES = [
    "http://media.idigbio.org/",
    "http://api.idigbio.org/v1/recordsets/",
    "https://api.idigbio.org/v2/media/",
    "http://api.idigbio.org/v2/media/",
    "http://www.tropicos.org/",
    "http://n2t.net/ark:/65665/" # Smithsonian
]


class Status(Enum):
    """An Enum of all the status we use, this is both ours and standard http

    For HTTP codes this isn't exhaustive, just ones we're likely to hit.
    """
    OK = 200

    # Theoretically we should be following this redirect, not getting this status
    SEE_OTHER = 303

    BAD_REQUEST = 400
    UNAUTHORIZED = 401
    FORBIDDEN = 403
    NOT_FOUND = 404
    METHOD_NOT_ALLOWED = 405
    NOT_ACCEPTABLE = 406
    REQUEST_TIMEOUT = 408
    GONE = 410
    UNSUPPORTED_MEDIA_TYPE = 415
    LEGAL_REASONS = 451

    SERVER_ERROR = 500
    NOT_IMPLEMENTED = 501
    BAD_GATEWAY = 502
    SERVICE_UNAVAILABLE = 503
    GATEWAY_TIMEOUT = 504
    HTTP_VERSION_NOT_SUPPORTED = 505
    INSUFFICIENT_STORAGE = 507
    LOOP_DETECTED = 508
    BANDWIDTH_LIMIT_EXCEEDED = 509

    UNHANDLED_FAILURE = 1000
    VALIDATION_FAILURE = 1001
    IGNORED = 1002
    UNREQUESTABLE = 1400
    FAUX_DENIED = 1403
    BLOCKED = 1509
    CONNECTION_ERROR = 1503
    STORAGE_ERROR = 2000

    #: This is for when we don't have the code in this Enum (and should probably add it)
    UNKNOWN = 9999
