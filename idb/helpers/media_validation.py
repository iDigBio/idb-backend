from __future__ import absolute_import
import magic
from . import conversions


def default_format_validator(url, t, fmt, content):
    mime = magic.from_buffer(content, mime=True)
    return (fmt == mime, mime)


def audio_mpeg3_validator(url, t, fmt, content):
    _, mime = default_format_validator(url, t, fmt, content)
    return (mime in ["audo/mpeg3", "audio/mpeg"], mime)


format_validators = {
    "model/mesh": lambda url, t, fmt, content: (
        url.endswith(".stl"), "model/mesh"),
    "audio/mpeg3": audio_mpeg3_validator
}


def get_validator(m):
    return format_validators.get(m, default_format_validator)

class MimeMismatchError(Exception):
    def __init__(self, expected_mime, detected_mime):
        self.args = (expected_mime, detected_mime)
        self.message = "Detected mime {0} doesn't match expected {1}".format(
            detected_mime, expected_mime)


class UnknownMediaTypeError(Exception):
    "Exception for unknown/undeterminable media, call with mime as the only arg"
    def __init__(self, mime):
        self.mime = mime
        self.args = (mime,)
        self.message = "Could not determine media type for mime: {0!r}".format(mime)

    def __str__(self):
        return self.message


def sniff_validation(content, raises=True):
    mime = magic.from_buffer(content, mime=True)
    mt = conversions.mime_mapping.get(mime)
    if not mt and raises:
        raise UnknownMediaTypeError(mime)
    return mime, mt


## SKETCH: I think this might make for a better *validation* pattern,
## haven't really gone through it all yet though.
# bucket_mimes = {
#     'images': {'image/jpeg', 'image/jp2'},
#     'sounds': {'audio/mpeg3', 'audio/mpeg'},
#     'models': {'model/mesh', 'text/plain'},
#     'video': {'video/mpeg', 'video/mp4'},
#     'datasets': {'text/csv', 'text/plain', 'application/zip'},
#     'debugfile': {'text/plain', 'application/zip'}
# }
# valid_buckets = set(bucket_mimes.keys())
