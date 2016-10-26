import itertools

def first(iterable, key=None, default=None):
    "Get the first Truthy value from an itererable"
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


def grouper(iterable, n, fillvalue=None):
    """Collect data into fixed-length chunks or blocks

    This is basedon the itertools doc
    """
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx
    args = [iter(iterable)] * n
    return itertools.izip_longest(fillvalue=fillvalue, *args)
