from __future__ import division, absolute_import, print_function


bucket_mimes = {
    'images': {'image/jpeg', 'image/jp2'},
    'sounds': {'audio/mpeg3', 'audio/mpeg'},
    'models': {'model/mesh', 'text/plain'},
    'video': {'video/mpeg', 'video/mp4'},
    'datasets': {'text/csv', 'text/plain', 'application/zip'},
    'debugfile': {'text/plain', 'application/zip'},
    'guoda': {'text/csv', 'text/plain', 'application/zip'},
}
valid_buckets = set(bucket_mimes.keys())

default_buckets = {
    "image/jpeg": "images",
    "image/jp2": "images",
    "audio/mpeg": "sounds",
    "video/mpeg": "video",
    "video/mp4": "video",
    "model/mesh": "models",
}

mime_aliases = {
    "audio/mpeg3": "audio/mpeg",
}


def get_default_bucket(mime):
    "Find the defualt bucket for given mime, None if indeterminate"
    return default_buckets.get(mime_aliases.get(mime, mime))


class MediaValidationError(Exception):
    def __str__(self):
        return self.message

class EtagMismatchError(MediaValidationError):
    def __init__(self, expected, calculated):
        self.args = (expected, calculated)
        self.message = "Calculated etag {0!r} doesn't match expected {1!r}" \
            .format(expected, calculated)

class InvalidBucketError(MediaValidationError):
    def __init__(self, bucket):
        self.args = (bucket,)
        self.message = "Invalid media type {0!r}".format(bucket)

class UnknownBucketError(MediaValidationError):
    def __init__(self, mime):
        self.args = (mime,)
        self.message = "Unknown media type for mime {0!r}".format(mime)

class MimeNotAllowedError(MediaValidationError):
    def __init__(self, mime, bucket):
        self.args = (mime, bucket)
        self.message = "Mime {0!r} not allowed in bucket {1!r}".format(
            mime, bucket)

class MimeMismatchError(MediaValidationError):
    def __init__(self, expected, detected):
        self.args = (expected, detected)
        self.message = "Detected mime {0} doesn't match expected {1}".format(
            detected, expected)


def sniff_mime(content):
    import magic
    return magic.from_buffer(content, mime=True)


def validate_mime_for_type(mime, t):
    """Check that the mime, and type are valid independently and together.

    Both arguments are nullable.
    """
    amime = mime_aliases.get(mime, mime)
    if t:
        if t not in valid_buckets:
            raise InvalidBucketError(t)
        if amime and amime not in bucket_mimes[t]:
            raise MimeNotAllowedError(mime, t)
    elif amime:
        t = get_default_bucket(amime)
        if not t:
            raise UnknownBucketError(mime)

    return amime, t


def validate(content, type=None, mime=None, url=None):
    """Validate the content with the given prior constraints

    If no constraints (type and mime) are given then just validate
    that the content is an accepted type with a default destination.
    """
    mime, type = validate_mime_for_type(mime, type)

    if url and url.endswith(".stl"):
        detected = "model/mesh"
    else:
        detected = sniff_mime(content)
    if not detected:
        raise MediaValidationError("Couldn't detect mime type")
    if mime and detected != mime_aliases.get(mime, mime):
        raise MimeMismatchError(mime, detected)

    return validate_mime_for_type(detected, type)
