from __future__ import absolute_import
import hashlib
import json
import sys

if sys.version_info >= (3, 5):
    from typing import Optional, Union


def calcEtag(data):
    arr = []
    for k in sorted(data.keys()):
        arr.append(k)
        arr.append(data[k])
    sha = hashlib.sha1()
    rs = json.dumps(arr, separators=(',', ':'), ensure_ascii=False)
    sha.update(rs.encode("utf8"))
    h = sha.hexdigest()
    return h


def calcFileHash(f, op=True, return_size=False):
    md5 = hashlib.md5()
    size = 0
    if op:
        with open(f, "rb") as fd:
            buf = fd.read(128)
            while len(buf) > 0:
                size += len(buf)
                md5.update(buf)
                buf = fd.read(128)
    else:
        buf = f.read(128)
        while len(buf) > 0:
            size += len(buf)
            md5.update(buf)
            buf = f.read(128)
    if return_size:
        return (md5.hexdigest(), size)
    else:
        return md5.hexdigest()


def objectHasher(
        hash_type, # type: str
        data, # type: Optional[Union[list, str, int, float, dict]]
        sort_arrays=False,
        sort_keys=True
    ): # type: (...) -> str
    """Converts ``data`` into a string to be hashed according to ``hash_type``.
    Falsy containers **or unsupported types** will result in the same hash as ``None``.

    :param hash_type: Hashing algorithm to use.
        See also: :py:const:`hashlib.algorithms_available`

    Examples
    ---
    The most-common use case of generating ETags:
    >>> k = {"dwc:genus": "Bonasa", "dwc:specificEpithet": "umbellus"}
    >>> objectHasher('sha256', k, sort_arrays=True)
    '3b4524f6a02903cdb88e0518e6f96f4ac2d2e10b7662d328251682bd7ea28489'

    Various falsy data yield the same result:
    >>> objectHasher('sha1', None)
    'da39a3ee5e6b4b0d3255bfef95601890afd80709'
    >>> objectHasher('sha1', None) \\
    ... == objectHasher('sha1', '') \\
    ... == objectHasher('sha1', []) \\
    ... == objectHasher('sha1', {})
    True

    Unsupported types will also yield the same result (and print the type):
    >>> objectHasher('sha1', objectHasher)
    <type 'function'>
    'da39a3ee5e6b4b0d3255bfef95601890afd80709'
    """

    h = hashlib.new(hash_type)

    s = ""
    if isinstance(data, list):
        sa = []
        for i in data:
            sa.append(objectHasher(hash_type,
                                   i,
                                   sort_arrays=sort_arrays,
                                   sort_keys=sort_keys))
        if sort_arrays:
            sa.sort()
        s = "".join(sa)

    elif isinstance(data, str) or isinstance(data, unicode):
        s = data
    elif isinstance(data, int) or isinstance(data, float):
        s = str(data)
    elif isinstance(data, dict):
        ks = data.keys()
        if sort_keys:
            ks.sort()

        for k in ks:
            s += k + objectHasher(hash_type,
                                  data[k],
                                  sort_arrays=sort_arrays,
                                  sort_keys=sort_keys)
    elif data is None:
        pass
    else:
        print type(data)

    # print s
    h.update(s.encode("utf8"))
    return h.hexdigest()
