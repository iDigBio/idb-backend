from __future__ import print_function
import zipfile
from lxml import etree
from collections import deque
import traceback
import shutil
import requests
from cStringIO import StringIO
import sys

if sys.version_info >= (3, 5):
    from typing import Any, Dict, List, Optional

from idb.helpers.logging import idblogger, getLogger
from .delimited import DelimitedFile
from idb.helpers.fieldnames import namespaces
from .xmlDictTools import xml2d

FNF_ERROR = "File {0} not present in archive."
DWC_SCHEMA_URL = "https://dwc.tdwg.org/text/tdwg_dwc_text.xsd"


def archiveFile(archive,name):
    # type: (zipfile.ZipFile, str) -> str
    """Within the given `archive`, returns the full path of the given file `name`.
    
    If not found or simply present at the zip file root,
    returns `name`.
    """
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
    extensions = None # type: List[DwcaRecordFile]

    def __init__(self,name="dwca.zip",skipeml=False,logname=None):
        self.path = name.split(".")[0]
        if self.path == name:
            self.path += "_extracted"

        if logname:
            logbase = getLogger(logname)
        else:
            logbase = idblogger.getChild('dwca')
        self.logger = logbase.getChild(name.split("/")[-1].split(".")[0])

        try:
            self.archive = zipfile.ZipFile(name, 'r')
            self.archive.extractall(self.path)
        except zipfile.BadZipfile:
            self.logger.fatal("Couldn't extract '%s'", name)
            raise

        root = None
        meta_filename = self.path + "/" + archiveFile(self.archive,"meta.xml")
        try:
            schema_parser = etree.XMLParser(no_network=False)
            r = requests.get(DWC_SCHEMA_URL)
            r_file_like_object = StringIO(r.content)
            parsed = etree.parse(r_file_like_object, schema_parser)
            schema = etree.XMLSchema(parsed)

            parser = etree.XMLParser(schema=schema, no_network=False)
            with open(meta_filename,'r') as meta:
                try:
                    root = etree.parse(meta, parser=parser).getroot()
                except:
                    self.logger.info("Schema validation failed against '%s', continuing unvalidated.", DWC_SCHEMA_URL)
                    self.logger.debug(traceback.format_exc())
                    meta.seek(0)
                    root = etree.parse(meta).getroot()
        except:
            self.logger.info("Failed to fetch schema '%s', continuing unvalidated.", DWC_SCHEMA_URL)
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
        self.core = DwcaRecordFile(self.archdict["core"],
                                   self.path + "/" + corefile,
                                   logname=self.logger.name)

        self.extensions = []
        if "extension" in self.archdict:
            if isinstance(self.archdict["extension"],list):
                for x in self.archdict["extension"]:
                    if isinstance(x["files"]["location"], list):
                        for loc in x["files"]["location"]:
                            extfile = archiveFile(self.archive,loc)
                            print(extfile)
                            try:
                                self.extensions.append(
                                    DwcaRecordFile(x,
                                                   self.path + "/" + extfile,
                                                   logname=self.logger.name))
                            except:
                                self.logger.error('Failed processing extension file (one of multiple locations) -- %s', extfile)
                                self.logger.debug(traceback.format_exc())
                    else:
                        extfile = archiveFile(self.archive,x["files"]["location"])
                        try:
                            self.extensions.append(
                                DwcaRecordFile(x,
                                               self.path + "/" + extfile,
                                               logname=self.logger.name))
                        except:
                            self.logger.error('Failed processing extension file (one of multiple extensions) -- %s', extfile)
                            self.logger.debug(traceback.format_exc())
            else:
                extfile = archiveFile(self.archive,self.archdict["extension"]["files"]["location"])
                self.extensions.append(
                    DwcaRecordFile(self.archdict["extension"],
                                   self.path + "/" + extfile,
                                   logname=self.logger.name))

    def close(self):
        shutil.rmtree(self.path)

class DwcaRecordFile(DelimitedFile):
    """
        Iterable. Internal representation of a darwin core archive record data file.
    """
    # Iterable capability inherited from DelimitedFile

    def __init__(self,filedict,fh,logname=None):
        # type: (Dict[str, Any], str, Optional[str]) -> None
        """
            Construct a DwcaRecordFile from a xml tree pointer to the <location> tag containing the data file name
            and a file handle or string pointing to the data file.

            Parameters
            ---
            filedict : dict
                dict representation of the Darwin Core metafile XML tree
                as returned by `lib.xmlDictTools.xml2d()`,
                specifically nodes <core> or <extension>
                and their contents
            fh : str
                File path to this record file
            logname : str, optional
        """

        # Avoid Setting attributes on self that conflict with attributes in DelimitedFile to enforce namespace separation
        if isinstance(filedict["files"]["location"], list):
            for l in filedict["files"]["location"]:
                if fh.endswith(l):
                    self.name = l
                    break
            else:
                raise Exception("Name not found.")
        else:
            self.name = filedict['files']['location']

        if logname:
            logbase = getLogger(logname)
        else:
            logbase = idblogger.getChild('dwca')
        self.logger = logbase.getChild(self.name.split(".")[0])

        fields = {}
        self.linebuf = deque()

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
        encoding = filedict.get("#encoding", "UTF-8")
        linesplit = get_unescaped_linesTerminatedBy(filedict)
        fieldsplit = get_unescaped_fieldsTerminatedBy(filedict)
        fieldenc = get_unescaped_fieldsEnclosedBy(filedict)
        ignoreheader = int(filedict.get("#ignoreHeaderLines","0"))

        self.defaults = {}
        if "field" not in filedict:
            filedict["field"] = []
        elif not isinstance(filedict['field'],list):
            filedict['field'] = [filedict['field']]
        for fld in filedict['field']:
            # drop any extra quote characters
            term = fld['#term'].replace("\"","")

            # Map fieldnames that contain a namespace to a CURIE form that includes the
            # shortened namespace alias and the term name (namespace:term).
            # e.g.   dwc:basisOfRecord
            # Sort by longest namespaces first
            ns_found = False
            for ns in sorted(namespaces.keys(),key=lambda x: len(x), reverse=True):
                if term.startswith(ns):
                    ns_found = True
                    term = term.replace(ns,namespaces[ns]+":")
                    break
            
            if not ns_found:
                self.logger.warning("Term '{0}' does not belong to a known namespace.".format(term))
                if "." in term:
                    # Due to downstream limitations of Elasticsearch, do not allow fieldnames containing dots.
                    # If we encounter a new field containing dots it usually means the namespace
                    # needs to be added to fieldnames.py.
                    self.logger.fatal("Term '{0}' contains a dot '.' which is not allowed in field names.".format(term))
                    raise Exception("Term '{0}' contains a dot '.' which is not allowed in field names.".format(term))

            self.logger.debug("Using term = '{0}'".format(term))
            if '#index' in fld:
                if int(fld['#index']) not in fields:
                    fields[int(fld['#index'])] = term
                else:
                    self.logger.error("Duplicate field index ignored {0}".format(str(fld)))
            if '#default' in fld:
                self.defaults[term] = fld['#default']

        super(DwcaRecordFile,self).__init__(
            fh,encoding=encoding,delimiter=fieldsplit,fieldenc=fieldenc,header=fields,rowtype=rowtype,
            logname=self.logger.name)

        while ignoreheader > 0:
            self._reader.next()
            ignoreheader -= 1

def get_unescaped_linesTerminatedBy(filedict):
    return filedict["#linesTerminatedBy"].decode('string_escape')

def get_unescaped_fieldsTerminatedBy(filedict):
    return filedict["#fieldsTerminatedBy"].decode('string_escape')

def get_unescaped_fieldsEnclosedBy(filedict):
    return filedict["#fieldsEnclosedBy"].decode('string_escape')

