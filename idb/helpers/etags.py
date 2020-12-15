from __future__ import absolute_import, print_function
import hashlib
import json


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


def objectHasher(hash_type, data, sort_arrays=False, sort_keys=True):
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
        print (type(data))

    # print s
    h.update(s.encode("utf8"))
    return h.hexdigest()
