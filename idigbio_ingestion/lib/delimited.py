import unicodecsv as csv
import traceback
import codecs
import io
import sys

from collections import defaultdict

if sys.version_info >= (3, 5):
    from typing import Dict, Optional
    DwcTerm = str

from idb.helpers.logging import idblogger, getLogger
from idb.helpers.fieldnames import NO_CLASS__UNKNOWN_FIELD, get_canonical_name, types

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

    def __init__(self,
            fh, # type: str
            encoding="utf8", # type: str
            delimiter=",", # type: str
            fieldenc="\"", # type: str
            header=None, # type: Optional[Dict[int, str]]
            rowtype=None, # type: Optional[str]
            logname=None # type: Optional[str]
        ):
        # type: (...) -> None
        """
        Parameters
        ---
        fh : str
            File path to this delimited file
        encoding : str
            Character encoding for this delimited file
        delimiter : str
            Delimiter between fields in this file.
            Corresponds to /*/@fieldsTerminatedBy in a Darwin Core metafile.
        fieldenc : str
            Character used to enclose (mark the start and end of) each field.
            Corresponds to /*/@fieldsEnclosedBy in a Darwin Core metafile.
        header : dict, optional
            Header of this delimited file,
            indexed by int, values are DwC terms (CURIE form, e.g. 'dwc:genus').
            If ``None``, the first row of this file will be interpreted as
            the header row.
            Corresponds to /*/field in a Darwin Core metafile.
        rowType : str, optional
            In URI form, the data class represented by each row of
            this delimited file.
            If ``None``, the rowtype will be inferred from the most common
            class each header term appears in.
            Corresponds to /*/@rowType in a Darwin Core metafile.
            Examples:
            - "http://data.ggbn.org/schemas/ggbn/terms/MaterialSample"
            - "http://rs.tdwg.org/ac/terms/Multimedia"

        logname : str, optional

        Notes
        ---
        Above corresponding XPaths derived from the Darwin Core text guide,
        version dated 2023-09-13, available at: https://dwc.tdwg.org/text/
        """
        super(DelimitedFile, self).__init__()

        # if incoming encoding is specified but is an empty string, we should abort here rather than
        # waiting for the actual file processing to raise an exception.
        if encoding == "":
            raise ValueError("Encoding cannot be an empty string, must specify an actual encoding.")
        else:
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

        # Count encountered Darwin Core classes.
        # To be used as a fallback if parameter 'rowtype' is unspecified.
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

        # early warning on unmapped fields
        unregistered_fields = set()
        for field in iter(self.fields.values()):
            if get_canonical_name(field)[1] == NO_CLASS__UNKNOWN_FIELD:
                unregistered_fields.add(field)
        if len(unregistered_fields) > 0:
            self.logger.warning("Encountered unmapped fields:\n  - " +
                "\n  - ".join(sorted(unregistered_fields)))

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
        # type: (...) -> Dict[DwcTerm, str]
        """
            Returns a parsed record line from a DWCA file as an dictionary
            of DwC terms and values
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
