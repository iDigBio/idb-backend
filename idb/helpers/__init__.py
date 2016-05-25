
def first(iterable, key=None, default=None):
    if key is None:
        for e in iterable:
            if e:
                return e
    else:
        for e in iterable:
            if key(e):
                return e
    return default


def ilen(iterable):
    "Traverse (possibly consuming) an iterable to count its length"
    count = 0
    for e in iterable:
        count += 1
    return count
