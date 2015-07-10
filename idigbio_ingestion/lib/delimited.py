import unicodecsv as csv
import traceback

from collections import defaultdict

import logging
from .log import getIDigBioLogger
from .fieldnames import get_canonical_name,types

class MissingFieldsException(Exception):
    def __init__(self,name,lineNumber,fieldnum,fieldname,lineArr):
        message = """
    File: {0}, Line: {1}
    Field Number: {2}, Field Key: {3}
    Line Array: {4}, Length: {5}
""".format(name,lineNumber,fieldnum,fieldname,repr(lineArr),len(lineArr))
        super(MissingFieldsException,self).__init__(message)

class LineLengthException(Exception):
    def __init__(self,name,lineNumber,lineLength,lineArr):
        message = """
    File: {0}, Line: {1}
    Expected Line Length: {2}, Actual Line Length: {4}
    Line Array: {3}
""".format(name,lineNumber,lineLength,repr(lineArr),len(lineArr))


class DelimitedFile(object):
    """
        Generic Delimited File class that returns lines as dicts of non-blank fields
    """

    def __init__(self,fh,encoding="utf8",delimiter=",",fieldenc="\"",header=None,rowtype=None,logname=None):
        super(DelimitedFile,self).__init__()

        self.encoding = encoding
        self.fieldenc = fieldenc
        self.delimiter = delimiter
        self.rowtype = rowtype
        self.lineCount = 0
        self.lineLength = None

        if isinstance(fh,str) or isinstance(fh,unicode):
            self.name = fh
            self.filehandle = open(fh,'rb')            
        else:
            self.name = fh.name
            self.filehandle = fh
        
        if logname is None:
            self.logger = getIDigBioLogger(self.name)
        else:
            self.logger = getIDigBioLogger(logname + "." + self.name)

        if self.fieldenc is None or self.fieldenc == "":
            self._reader = csv.reader(self.filehandle,encoding=self.encoding,delimiter=self.delimiter,quoting=csv.QUOTE_NONE)
        else:
            self._reader = csv.reader(self.filehandle,encoding=self.encoding,delimiter=self.delimiter,quotechar=self.fieldenc)

        t = defaultdict(int)
        if header is not None:
            self.fields = header
            for k,v in header.items():
                cn = get_canonical_name(v)
                t[cn[1]] += 1            
        else:
            headerline = self._reader.next()
            self.lineLength = len(headerline)
            self.fields = {}
            for k,v in enumerate(headerline):
                cn = get_canonical_name(v)
                if cn[0] is not None:
                    t[cn[1]] += 1                
                    self.fields[k] = cn[0]

        if self.rowtype is None:
            items = t.items()
            items.sort(key=lambda item: (item[1], item[0]), reverse=True)
            self.rowtype = items[0][0]
        elif self.rowtype in types:
            self.rowtype = types[self.rowtype]["shortname"]


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
        closed = self.filehandle.closed

    def next(self):
        """
            Returns the next line in the record file, used for iteration
        """
        return self.readline()

    def readline(self,size=None):
        """
            Returns a parsed record line from a DWCA file as an dictionary.
        """

        lineDict = None
        while lineDict is None:     
            try:                
                lineArr = self._reader.next()
                self.lineCount += 1
                if self.lineLength is None:
                    self.lineLength = len(lineArr)
                elif self.lineLength != len(lineArr):
                    raise LineLengthException(self.name,self.lineCount,self.lineLength,lineArr)
                
                lineDict = {}
                for k in self.fields:
                    try:
                        lineArr[k] = lineArr[k].strip()
                        if lineArr[k] != "":
                            lineDict[self.fields[k]] = lineArr[k]
                    except IndexError, e:                        
                        raise MissingFieldsException(self.name,self.lineCount,k,self.fields[k],lineArr)
                return lineDict
            except UnicodeDecodeError:
                lineDict = None
                self.lineCount += 1
                self.logger.warn("Unicode Decode Exception: {0} Line {1}".format(self.name,self.lineCount))
                self.logger.debug(traceback.format_exc())                
            except MissingFieldsException:
                lineDict = None
                self.logger.warn("Missing Fields Exception: {0} Line {1}".format(self.name,self.lineCount))
                self.logger.debug(lineArr)
                self.logger.debug(traceback.format_exc())
            except LineLengthException:
                lineDict = None
                self.logger.warn("LineLengthException: {0} Line {1} ({2},{3})".format(self.name,self.lineCount,self.lineLength,len(lineArr)))
                self.logger.debug(lineArr)
                self.logger.debug(traceback.format_exc())
        return lineDict

    def readlines(self,sizehint=None):
        """
            Returns all lines in the file. Cheats off readline.
        """
        lines = []
        for line in self:
            lines.append(self.readline())
        return lines