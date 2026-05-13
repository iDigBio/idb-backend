import os
import re
import csv
import traceback
import codecs
import io
import sys

from collections import defaultdict

if sys.version_info >= (3, 5):
    from typing import Dict, Optional
    DwcTerm = str

from idb.helpers.logging import idblogger, getLogger
from idb.helpers.fieldnames import get_canonical_name, types
#For POLYGON fields and others which are large
csv.field_size_limit(sys.maxsize)

class MissingFieldsException(Exception):

    def __init__(self, name, lineNumber, fieldnum, fieldname, lineArr):
        message = """
    File: {0}, Line: {1}
    Field Number: {2}, Field Key: {3}
    Line Array: {4}, Length: {5}
""".format(name, lineNumber, fieldnum, fieldname, repr(lineArr), len(lineArr))
        super(MissingFieldsException, self).__init__(message)


class LineLengthException(Exception):

    def __init__(self, name, lineNumber, lineLength, lineArr):
        message = """
    File: {0}, Line: {1}
    Expected Line Length: {2}, Actual Line Length: {4}
    Line Array: {3}
""".format(name, lineNumber, lineLength, repr(lineArr), len(lineArr))
        super(LineLengthException, self).__init__(message)


def flag_unicode_error(e):
    bad_chars = "".join([hex(ord(c)) for c in e.object[e.start:e.end]])
    return (u"DECODEERROR:" + bad_chars, e.end)

codecs.register_error("flag_error", flag_unicode_error)


class DelimitedFile(object):
    """
        Iterable. Generic Delimited File class that returns lines as dicts of non-blank fields
    """

    def __iter__(self):
        return self

    def __next__(self):
        return super(DwcaRecordFile, self).next()   # or whatever transform DelimitedFile expects

    # optional Py2 compat (harmless in Py3, but don’t do this if you also define def next())
    next = __next__

    def _normalize_delimiter(self, d, default=","):
        """
        Normalize DwC-A / meta.xml delimiter representations into a single character str.
        Accepts things like:
          - "\t" or "\\t" (literal backslash+t)
          - "tab" / "TAB"
          - "0x09"
          - "&#9;" or "&#x9;"
          - "||" (collapses to "|") when all chars are identical
        """
        if d is None:
            return default

        # bytes -> str
        if isinstance(d, (bytes, bytearray, memoryview)):
            d = bytes(d).decode("utf-8", errors="replace")

        d = str(d).strip()

        # strip simple wrapping quotes: '"|"' or "'\t'"
        if len(d) >= 2 and d[0] == d[-1] and d[0] in ("'", '"'):
            d = d[1:-1]

        # common words
        if d.lower() == "tab":
            d = "\t"

        # literal backslash escapes often found in meta.xml parsing
        if d in (r"\t", "\\t"):
            d = "\t"
        elif d in (r"\n", "\\n"):
            d = "\n"
        elif d in (r"\r", "\\r"):
            d = "\r"

        # numeric forms: 0x09
        m = re.fullmatch(r"0x([0-9a-fA-F]+)", d)
        if m:
            d = chr(int(m.group(1), 16))

        # XML numeric entities: &#9; or &#x9;
        m = re.fullmatch(r"&#([0-9]+);", d)
        if m:
            d = chr(int(m.group(1), 10))
        m = re.fullmatch(r"&#x([0-9a-fA-F]+);", d)
        if m:
            d = chr(int(m.group(1), 16))

        # empty -> default
        if d == "":
            return default

        # If it's multiple identical chars (e.g., "||" or ",,") collapse to one.
        if len(d) != 1:
            uniq = set(d)
            if len(uniq) == 1:
                d = d[0]

        if len(d) != 1:
            # This is the place to log what you got from meta.xml
            raise ValueError(f"Invalid CSV delimiter {d!r} (expected 1 character)")

        return d

    def __init__(self, fh, encoding="utf-8", delimiter=",", fieldenc='"',
                 header=None, rowtype=None, logname=None):
        super(DelimitedFile, self).__init__()

        if encoding == "":
            raise ValueError("Encoding cannot be an empty string, must specify an actual encoding.")
        self.encoding = encoding

        self.fieldenc = fieldenc
        self.delimiter = self._normalize_delimiter(delimiter)
        self.rowtype = rowtype
        self.lineCount = 0
        self.lineLength = None

        # Python 3: no 'unicode'. Use str / PathLike.
        is_path = isinstance(fh, (str, os.PathLike))

        if is_path:
            self.name = os.fspath(fh)
            self.filehandle = io.open(self.name, "r", encoding=encoding, errors="flag_error", newline="")
        else:
            # assume a file-like object already opened in text mode
            self.filehandle = fh
            self.name = getattr(fh, "name", "<stream>")

        if logname is None:
            self.logger = idblogger.getChild("df")
        else:
            self.logger = getLogger(logname)

        # IMPORTANT: csv.reader in Python 3 consumes TEXT lines (str), not bytes.
        if self.fieldenc is None or self.fieldenc == "":
            self._reader = csv.reader(
                self.filehandle,
                delimiter=self.delimiter,
                quoting=csv.QUOTE_NONE,
            )
        else:
            self._reader = csv.reader(
                self.filehandle,
                delimiter=self.delimiter,
                quotechar=self.fieldenc,
            )

        # Count encountered Darwin Core classes.
        # To be used as a fallback if parameter 'rowtype' is unspecified.
        t = defaultdict(int)

        if header is not None:
            self.fields = header
            for _, v in header.items():
                cn = get_canonical_name(v)
                t[cn[1]] += 1
        else:
            # Python 3: next(reader), not reader.next()
            headerline = next(self._reader)
            self.lineLength = len(headerline)
            self.fields = {}
            for k, v in enumerate(headerline):
                cn = get_canonical_name(v)
                if cn[0] is not None:
                    t[cn[1]] += 1
                    self.fields[k] = cn[0]

        # early warning on unmapped fields
        unregistered_fields = set()
        for field in iter(self.fields.values()):
            if get_canonical_name(field)[1] == NO_CLASS__UNKNOWN_FIELD:
                unregistered_fields.add(field)
        if len(unregistered_fields) > 0:
            self.logger.warning("Encountered unmapped fields:\n  - " +
                "\n  - ".join(sorted(unregistered_fields)))

        if self.rowtype is None:
            # Python 3: dict_items is not sortable in-place; use sorted()
            items = sorted(t.items(), key=lambda item: (item[1], item[0]), reverse=True)
            self.rowtype = items[0][0]
            self.logger.info("Setting row type to %s", self.rowtype)
        elif self.rowtype in types:
            self.rowtype = types[self.rowtype]["shortname"]
        else:
            raise TypeError("{} not mapped to short name".format(self.rowtype))



    def __iter__(self):
        """
            Returns the object itself, as per spec.
        """
        return self

    def close(self):
        """
            Closes the internally maintained filehandle
        """
        self.filehandle.close()

    def next(self):
        """
            Returns the next line in the record file, used for iteration
        """
        return self.readline()

    def readline(self, size=None):
        # type: (...) -> Dict[DwcTerm, str]
        """
            Returns a parsed record line from a DWCA file as an dictionary
            of DwC terms and values
        """

        while True:
            try:
                lineDict = {}
                # self.filehandle.snap()
                lineArr = next(self._reader)

                self.lineCount += 1
                if self.lineLength is None:
                    self.lineLength = len(lineArr)
                elif self.lineLength != len(lineArr):
                    raise LineLengthException(
                        self.name, self.lineCount, self.lineLength, lineArr)

                for k in self.fields:
                    if k >= len(lineArr):
                        raise MissingFieldsException(
                            self.name, self.lineCount, k, self.fields[k], lineArr)

                    lineArr[k] = lineArr[k].strip()
                    if "DECODEERROR:" in lineArr[k]:
                        lineDict["flag_encoding_error"] = True
                        self.logger.warn(
                            "Unicode Decode Exception: %s Line %s, Field %s, Value: %s",  # noqa
                            self.name,
                            self.lineCount,
                            self.fields[k],
                            lineArr[k]
                        )
                        lineDict[self.fields[k]] = lineArr[
                            k].replace("DECODEERROR:", "")
                    elif lineArr[k] != "":
                        lineDict[self.fields[k]] = lineArr[k]

                return lineDict
            except UnicodeDecodeError:
                # This should never Happen
                raise
            except MissingFieldsException:
                self.logger.warn("Missing Fields Exception: {0} Line {1}".format(
                    self.name, self.lineCount))
                self.logger.debug(lineArr)
                self.logger.info(traceback.format_exc())
            except LineLengthException:
                self.logger.warn("LineLengthException: {0} Line {1} ({2},{3})".format(
                    self.name, self.lineCount, self.lineLength, len(lineArr)))
                self.logger.debug(lineArr)
                self.logger.info(traceback.format_exc())
            except csv.Error:
                # This catches NUL Byte errors and adds a bit of danger
                self.logger.error('csv ERROR at line {0}, possibly a NUL Byte or encoding mismatch'.format(self.lineCount))
                self.logger.debug(traceback.format_exc())
            except StopIteration:
                self.logger.debug("Finished File (StopIteration reached)")
                raise
            except Exception as e:
                self.logger.warn("OtherException {3}: {0} Line {1} ({2})".format(
                    self.name, self.lineCount, self.lineLength, str(e)))
                self.logger.info(traceback.format_exc())
                raise
        return lineDict

    def readlines(self, sizehint=None):
        """
            Returns all lines in the file. Cheats off readline.
        """
        lines = []
        for line in self:
            lines.append(self.readline())
        return lines
