from __future__ import division, absolute_import, print_function

IGNORE_PREFIXES = [
    "http://media.idigbio.org/",
    "http://www.tropicos.org/"
]

def check_ignore_media(url):
    for p in IGNORE_PREFIXES:
        if url.startswith(p):
            return True
    return False
