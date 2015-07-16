import zipfile
from lxml import etree
import sys
from collections import deque
import os
import chardet
import traceback
import shutil

from .log import getIDigBioLogger
from .delimited import DelimitedFile
from idb.helpers.fieldnames import namespaces
from .xmlDictTools import xml2d

FNF_ERROR = "File {0} not present in archive."


def archiveFile(archive,name):
    metaname = name
    for f in archive.namelist():
        if f.endswith("/" + name):
            metaname = f
    return metaname

class Dwca(object):
    """
        Internal representation of a Darwin Core Archive file.
    """
    
    archdict = None
    archive = None   
    metadata = None
    core = None
    extensions = None

    def __init__(self,name="dwca.zip",skipeml=False,logname=None):
        self.archive = zipfile.ZipFile(name, 'r')

        self.path = name.split(".")[0]

        if self.path == name:
            self.path += "_extracted"

        self.archive.extractall(self.path)

        if logname is None:
            self.logger = getIDigBioLogger(name.split(".")[0])
            self.logname = name.split(".")[0]
        else:
            self.logger = getIDigBioLogger(logname + "." + name.split(".")[0])
            self.logname = logname + "." + name.split(".")[0]

        root=None
        meta_filename = self.path + "/" + archiveFile(self.archive,"meta.xml")
        try:
            schema_parser = etree.XMLParser(no_network=False)
            schema = etree.XMLSchema(etree.parse("http://rs.tdwg.org/dwc/text/tdwg_dwc_text.xsd", parser=schema_parser))
            parser = etree.XMLParser(schema=schema, no_network=False)

            with open(meta_filename,'r') as meta:
                try:
                    root = etree.parse(meta, parser=parser).getroot()
                except:
                    self.logger.info("Schema validation failed, continuing unvalidated")
                    self.logger.debug(traceback.format_exc())
                    meta.seek(0)
                    # print meta.read()
                    # meta.seek(0)
                    root = etree.parse(meta).getroot()
        except:
            self.logger.info("Failed to fetch schema, continuing unvalidated")
            self.logger.debug(traceback.format_exc())
            with open(meta_filename,'r') as meta:
                root = etree.parse(meta).getroot()
        rdict = xml2d(root)

        self.archdict = rdict["archive"]

        if not skipeml and "#metadata" in self.archdict:
            metadata = archiveFile(self.archive,self.archdict["#metadata"])
            with open(self.path + "/" + metadata,'r') as mf:
                mdtree = etree.parse(mf).getroot()
                self.metadata = xml2d(mdtree)
        else:
            self.metadata = None

        corefile = archiveFile(self.archive,self.archdict["core"]["files"]["location"])
        self.core = DwcaRecordFile(self.archdict["core"], self.path + "/" + corefile,logname=self.logname)
        
        self.extensions = []
        if "extension" in self.archdict:
            if isinstance(self.archdict["extension"],list):
                for x in self.archdict["extension"]:
                    extfile = archiveFile(self.archive,x["files"]["location"])
                    try:
                        self.extensions.append(DwcaRecordFile(x, self.path + "/" + extfile,logname=self.logname))
                    except:
                        pass
            else:            
                extfile = archiveFile(self.archive,self.archdict["extension"]["files"]["location"])
                self.extensions.append(DwcaRecordFile(self.archdict["extension"], self.path + "/" + extfile,logname=self.logname))

    def close(self):
        shutil.rmtree(self.path)

class DwcaRecordFile(DelimitedFile):
    """
        Internal representation of a darwin core archive record data file.
    """

    def __init__(self,filedict,fh,logname=None):
        """
            Construct a DwcaRecordFile from a xml tree pointer to the <location> tag containing the data file name
            and a file handle or string pointing to the data file.
        """

        # Avoid Setting attributes on self that conflict with attributes in DelimitedFile to enforce namespace separation
        self.name = filedict['files']['location']

        if logname is None:
            self.logger = getIDigBioLogger(name.split(".")[0])
            self.logname = self.name.split(".")[0]
        else:
            self.logger = getIDigBioLogger(logname + "." + self.name.split(".")[0])
            self.logname = logname + "." + self.name.split(".")[0]

        fields = {}
        self.linebuf = deque()
        closed = False

        idtag = "id"
        idfld = None
        if 'id' in filedict:
            self.filetype = "core"
            idfld = filedict["id"]
        elif "coreid" in filedict:
            idtag = "coreid"
            idfld = filedict["coreid"]
            self.filetype = "extension"
        else:
            self.filetype = "core"

        if idfld is not None:
            fields[int(idfld['#index'])] = idtag

        rowtype = filedict["#rowType"]
        encoding = filedict["#encoding"]
        linesplit = filedict["#linesTerminatedBy"].decode('string_escape') 
        fieldsplit = filedict["#fieldsTerminatedBy"].decode('string_escape') 
        fieldenc = filedict["#fieldsEnclosedBy"].decode('string_escape') 
        ignoreheader = int(filedict["#ignoreHeaderLines"])
        
        self.defaults = {}
        if not isinstance(filedict['field'],list):
            filedict['field'] = [filedict['field']]
        for fld in filedict['field']:
            # drop any extra quote characters
            term = fld['#term'].replace("\"","")

            # map xmp namespaces into short code form (xxx:fieldName), longest namespaces first
            for ns in sorted(namespaces.keys(),key=lambda x: len(x), reverse=True):
                if term.startswith(ns):
                    term = term.replace(ns,namespaces[ns]+":")
                    break
            if '#index' in fld:
                if int(fld['#index']) not in fields:
                    fields[int(fld['#index'])] = term
                else:
                    self.logger.error("Duplicate field index ignored {0}".format(str(fld)))
            if '#default' in fld:
                self.defaults[term] = fld['#default']
        # print self.defaults

        super(DwcaRecordFile,self).__init__(fh,encoding=encoding,delimiter=fieldsplit,fieldenc=fieldenc,header=fields,rowtype=rowtype,logname=logname)

        while ignoreheader > 0:
            _ = self._reader.next()    
            ignoreheader -= 1


    def readline(self,size=None):
        lineDict = {}
        lineDict.update(self.defaults)
        lineDict.update(super(DwcaRecordFile,self).readline(size))
        return lineDict
