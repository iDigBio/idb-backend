import unicodecsv as csv
import traceback
import codecs
import io

from collections import defaultdict

from idb.helpers.logging import idblogger, getLogger
from idb.helpers.fieldnames import get_canonical_name, types

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
        Generic Delimited File class that returns lines as dicts of non-blank fields
    """

    def __init__(self, fh, encoding="utf8", delimiter=",", fieldenc="\"", header=None, rowtype=None, logname=None):
        super(DelimitedFile, self).__init__()

        self.encoding = encoding
        self.fieldenc = fieldenc
        self.delimiter = delimiter
        self.rowtype = rowtype
        self.lineCount = 0
        self.lineLength = None

        if isinstance(fh, str) or isinstance(fh, unicode):
            self.name = fh
        else:
            self.name = fh.name
        self.filehandle = io.open(
            fh, "r", encoding=encoding, errors="flag_error")

        if logname is None:
            self.logger = idblogger.getChild('df')
        else:
            self.logger = getLogger(logname)

        encoded_lines = (l.encode("utf-8") for l in self.filehandle)
        if self.fieldenc is None or self.fieldenc == "":
            self._reader = csv.reader(encoded_lines, encoding="utf-8",
                                      delimiter=self.delimiter, quoting=csv.QUOTE_NONE)
        else:
            self._reader = csv.reader(encoded_lines, encoding="utf-8",
                                      delimiter=self.delimiter, quotechar=self.fieldenc)

        t = defaultdict(int)
        if header is not None:
            self.fields = header
            for k, v in header.items():
                cn = get_canonical_name(v)
                t[cn[1]] += 1
        else:
            headerline = self._reader.next()
            self.lineLength = len(headerline)
            self.fields = {}
            for k, v in enumerate(headerline):
                cn = get_canonical_name(v)
                if cn[0] is not None:
                    t[cn[1]] += 1
                    self.fields[k] = cn[0]

        if self.rowtype is None:
            items = t.items()
            items.sort(key=lambda item: (item[1], item[0]), reverse=True)
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
        """
            Returns a parsed record line from a DWCA file as an dictionary.
        """

        while True:
            try:
                lineDict = {}
                # self.filehandle.snap()
                lineArr = self._reader.next()

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
