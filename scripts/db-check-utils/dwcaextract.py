#!/usr/bin/env python3

#python-version: 3.8   # reason: version used in Ubuntu 20.04
#requirements:
# defusedxml==0.7.1 # for handling untrusted XML

"""
.. module:: dwcaextract
:synopsis: Reads Darwin Core Archive files, dumps specified contents
"""
# whoops i accidentally a dwca parser

from __future__ import annotations

__version__ = "0.2.0.20240712"

from defusedxml.ElementTree import parse as detxmlparse

import argparse
import copy
import csv
import itertools
import io
import json
import re
import sys
import textwrap
import traceback
import uuid
import zipfile
from typing import TYPE_CHECKING, NamedTuple, TypedDict
from urllib.parse import urlparse

if TYPE_CHECKING:
    from _typeshed import FileDescriptorOrPath
    from collections.abc import Callable
    from io import TextIOWrapper
    from os import PathLike
    from xml.etree.ElementTree import ElementTree


PRINT_ON_NTH_CONSECUTIVE_CSV_ERROR = (1, 3, 5, 10)

# defined as constants to force documentation to match implementation
FMT_DUMP_MULTIMEDIA_URLS_OUTPUT = '%(archive_path)s: %(type)s = %(url)s'
FMT_DUMP_DWC_FIELD_OUTPUT = '%(archive_path)s:%(subfile)s (%(rowtype)s): %(coltype)s = %(value)s'

def main():
    argparser = argparse.ArgumentParser(
        description='Darwin Core Archive (DwC-A) .zip processor',
        epilog=textwrap.dedent("""\
            example usage:
              Many instances of this script can be run independently of one
              another, allowing for bulk processing of input files.
              For the examples below, adjust `xargs -P` for the number of
              concurrent processes to run.

              Dumping fields to JSON file for many meta.xml files:
              $ find -name meta.xml -print0 \\
                | xargs -0 -n 1 -P 15 -I % \\
                -- bash -c './process_meta.py dump-fields -o %{-fields.json,}'

              Dumping multimedia URLs from many DwC-A .zip archives:
              (no temporary file extraction needed):
              $ find /path/to/dwca-collection-dir -maxdepth 1 -regextype sed \\
                  -regex '.*/[a-f0-9\-]\{36\}' -print0 \\
                | xargs -0 -n 1 -P 15 -I % \\
                  -- bash -c './process_meta.py dump-multimedia-urls %'
            """),
        formatter_class=argparse.RawDescriptionHelpFormatter)
    subparsers = argparser.add_subparsers(required=True, metavar='OPERATION')

    parser_dump_fields = subparsers.add_parser('dump-fields',
        description="Dumps DwC fields from meta.xml to JSON",
        help='dump meta.xml DwC fields to JSON')
    parser_dump_fields.add_argument('infile',
        help='input meta.xml file path',
        metavar='META-XML-PATH')
    parser_dump_fields.add_argument('-o', '--output',
        default='-',
        type=argparse.FileType('wt'),
        help='write output to %(metavar)s instead of stdout',
        metavar='FILE')
    parser_dump_fields.add_argument('-x', '--exclude-extension',
        action='append',
        default=[],
        help='example: http://rs.tdwg.org/ac/terms/Multimedia',
        metavar='TYPE')
    parser_dump_fields.set_defaults(func=dump_dwc_fields)

    parser_dump_multimedia_urls = subparsers.add_parser('dump-multimedia-urls',
        description=textwrap.dedent(f"""\
            Dumps multimedia URLs from the Multimedia extension (if available)

            output format: {FMT_DUMP_MULTIMEDIA_URLS_OUTPUT % {'archive_path': '<dwca-zip-path>', 'type': '<column-type>', 'url': '<media-url>'}}
            """),
        help='dump multimedia URLs from DwC-A',
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser_dump_multimedia_urls.add_argument('inzip',
        help='input DwC-A .zip archive',
        metavar='DWCA-ZIP')
    parser_dump_multimedia_urls.add_argument('-o', '--output',
        default='-',
        type=argparse.FileType('wt'),
        help='write output to %(metavar)s instead of stdout',
        metavar='FILE')
    parser_dump_multimedia_urls.add_argument('--strict',
        action='store_true',
        help='report errors on bad CSV input')
    parser_dump_multimedia_urls.set_defaults(func=dump_multimedia_urls)

    parser_field_score = subparsers.add_parser('field-count',
        description='Given a meta.xml and a list of fields to match, counts how many fields are present in meta.xml',
        help='counts desired meta.xml DwC fields')
    parser_field_score.add_argument('-l', '--list-fields',
        action='store_true',
        help='also print all matching fields',)
    parser_field_score.add_argument('infile_metaxml',
        help='input meta.xml file path',
        metavar='META-XML-PATH')
    parser_field_score.add_argument('infile_fields',
        help='list of fields to count; line-delimited (example entry: http://purl.org/dc/terms/modified)',
        metavar='FIELDS-PATH')
    parser_field_score.set_defaults(func=count_dwc_fields)

    parser_get_field = subparsers.add_parser('get-value',
        description='Dumps values for any specified DwC field as JSONL',
        help='reads DwC-A, outputs found field values',
        epilog=textwrap.dedent(f"""\
            search specification [JSON]: '{{DOCUMENT: TERM}}'
            where:
              DOCUMENT is an IRI (e.g. "http://rs.tdwg.org/dwc/terms/Occurrence"),
              or "*" to mean any document
              TERM is either a list
              (`["http://rs.tdwg.org/dwc/terms/eventRemarks", "http://rs.tdwg.org/dwc/terms/parentEventID"]`)
              or a single item (`"http://purl.org/dc/terms/type"`)

            example:
            Bringing the above all together:
            '{{ "http://rs.tdwg.org/dwc/terms/Occurrence": ["http://rs.tdwg.org/dwc/terms/eventRemarks", "http://rs.tdwg.org/dwc/terms/parentEventID"], "*": "http://purl.org/dc/terms/type"}}'
            This example searches for dwc:eventRemarks and dwc:parentEventID under Occurrence records,
            and dcterms:type under any DwC-A component

            output format: {FMT_DUMP_DWC_FIELD_OUTPUT %
                {'archive_path': '<dwca-zip-path>',
                    'subfile': '<zip-subfile>',
                    'rowtype': '<row-type>',
                    'coltype': '<column-type>',
                    'value': '<value>'}}
            """),
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser_get_field.add_argument('inzip',
        help='input DwC-A .zip archive',
        metavar='DWCA-ZIP')
    parser_get_field.add_argument('fields',
        help='field(s) to search for (see "section search specification")',
        metavar='FIELDS')
    parser_get_field.add_argument('-o', '--output',
        default='-',
        type=argparse.FileType('wt'),
        help='write output to %(metavar)s instead of stdout',
        metavar='FILE')
    parser_get_field.add_argument('--strict',
        action='store_true',
        help='report errors on bad CSV input')
    parser_get_field.set_defaults(func=get_dwc_field)

    args = argparser.parse_args()
    argsdict = vars(args)
    return args.func(**{k: argsdict[k] for k in argsdict if k != 'func'})



NS_DWCA = '{http://rs.tdwg.org/dwc/text/}'

class DwcaProcessingError(Exception):
    """Base class for exceptions in this module."""


class DwcaExtensionFormatAttributes(TypedDict):
    encoding: str
    fieldsEnclosedBy: str
    fieldsTerminatedBy: str
    ignoreHeaderLines: int
    linesTerminatedBy: str #NOTE# as of python-3.8, ignored by csv.reader
    rowType: str
    targetColumns: list[tuple[int, str]]

# defaults from Darwin Core Maintenance Group. 2023. Darwin Core text guide.
# Biodiversity Information Standards (TDWG). http://rs.tdwg.org/dwc/terms/guides/text/2023-09-13
# (see sect. 2.2.1 ("Metafile content: The <core> or <extension> element: Attributes"))
# except 'indices', which is to be modified at runtime
CSV_FORMAT_PARAMETERS_DEFAULT: DwcaExtensionFormatAttributes = {
    'encoding': 'utf-8',
    'fieldsEnclosedBy': '"',
    'fieldsTerminatedBy': ',',
    'ignoreHeaderLines': 0,
    'linesTerminatedBy': '\n', #NOTE# as of python-3.8, ignored by csv.reader
    'rowType': None, # required by DwC specification, this better be overwritten
    'targetColumns': [],
}

def dwca_file_attributes_to_csv_dialect_parameters(params: DwcaExtensionFormatAttributes) -> dict:
    """Translates DwC-A file attributes into parameters to be passed into `csv.reader()`"""
    # translates escape sequences such as "\\t" into "\t"
    unescape_str: Callable[[str], str] = lambda s: bytes(s, 'utf-8').decode('unicode_escape')
    return {
        'delimiter': unescape_str(params['fieldsTerminatedBy']),
        'lineterminator': unescape_str(params['linesTerminatedBy']), #NOTE# as of python-3.8, ignored by csv.reader
        'quotechar': unescape_str(params['fieldsEnclosedBy']),
    }


def meta_xml_syntax_check(metaxml: ElementTree, filepath: str):
    """Raises exception if a meta.xml syntax error is found. `filepath` is used to prefix the exception message."""
    # I'm working with a valid file, right?
    xmlroot = metaxml.getroot()
    if xmlroot.tag != f'{NS_DWCA}archive':
        raise KeyError(f'{filepath}: invalid meta.xml: missing root <archive>')
    for dwcacomponent in xmlroot:
        if dwcacomponent.tag not in (f'{NS_DWCA}core', f'{NS_DWCA}extension'):
            raise KeyError(f'{filepath}: invalid meta.xml: encountered unexpected component <{dwcacomponent.tag}>')

def count_dwc_fields(infile_metaxml: FileDescriptorOrPath, infile_fields: FileDescriptorOrPath, list_fields: bool):
    """(see argparse help at `parser_field_score` above)"""
    metaxet = detxmlparse(infile_metaxml)
    meta_xml_syntax_check(metaxet, str(infile_metaxml))
    target_fields = set(open(infile_fields, 'rt').read().splitlines())
    seen_fields = set()
    metaxmlroot = metaxet.getroot()
    for dwcacomponent in metaxmlroot:
        try:
            rowtype = dwcacomponent.get('rowType')
            for element in dwcacomponent:
                if element.tag == f'{NS_DWCA}field':
                    term = element.get('term')
                    if term in target_fields:
                        seen_fields.add(term)
        except Exception as exc:
            raise DwcaProcessingError(f'{infile_metaxml}: unhandled exception while processing meta.xml') from exc

    # prepare strings to be output all at once
    # (to allow parallel script runs to not clobber each other with stdout)
    if list_fields:
        # ...but if --list-fields is specified, can't do anything about them mixing together, though
        # (resulting multi-line string probably too long to be atomically written to stdout)
        for f in seen_fields:
            strout = f'{infile_metaxml}: {f}'
            print(strout)
    outmsg = f'{len(seen_fields)} {infile_metaxml}'
    print(outmsg)


def dump_dwc_fields(infile: FileDescriptorOrPath, output: TextIOWrapper, exclude_extension: list[str]):
    """(see argparse help text at `parser_dump_fields` above)"""
    metaxet = detxmlparse(infile)
    meta_xml_syntax_check(metaxet, str(infile))
    metaxmlroot = metaxet.getroot()
    outdict = dict()
    for dwcacomponent in metaxmlroot:
        fields = list()
        try:
            rowtype = dwcacomponent.get('rowType')
            if rowtype in exclude_extension:
                continue
            for element in dwcacomponent:
                if element.tag == f'{NS_DWCA}field':
                    fields.append({"field": element.get('term')})
                elif element.tag == f'{NS_DWCA}files':
                    continue
                elif element.tag == f'{NS_DWCA}coreid':
                    fields.append({"coreid": ""})
                elif element.tag == f'{NS_DWCA}id':
                    fields.append({"id": ""})
                else:
                    raise KeyError(f'{infile}: invalid meta.xml: encountered unexpected <{dwcacomponent.tag[len(NS_DWCA):]} rowType="{rowtype}"> element <{element.tag}>')
        except Exception as exc:
            fields.append({"_error": str(exc)})
            raise DwcaProcessingError(f'{infile}: unhandled exception while processing meta.xml') from exc
        if 'rowtype' not in locals():
            rowtype = f'_error_rowtype_lookup_failed_{str(uuid.uuid4()).replace("-","_")}'
        outdict[rowtype] = fields
    #end for each DwC-A component
    jsonstr = json.dumps(outdict, indent=2)
    # finishing, finely folds fields for fewer file flines
    output.write(re.sub(r'{\s+"(field|(?:core)?id)": (\S+)\s+}', r'{ "\1": \2 }', jsonstr))

def dump_multimedia_urls(inzip: FileDescriptorOrPath, output: TextIOWrapper, strict = False):
    """(see argparse help at `parser_dump_multimedia_urls` above)"""
    # ordered from high-to-low priority,
    # according to github:iDigBio/idb-backend:idb/helpers/conversions.py: def get_accessuri()
    # as of git-commit-id 5b088c0 (dated 10 Apr 2024)
    TARGET_MULTIMEDIA_TERMS = (
        'http://rs.tdwg.org/ac/terms/accessURI',
        'http://rs.tdwg.org/ac/terms/bestQualityAccessURI',
        'http://purl.org/dc/terms/identifier',
        'http://purl.org/dc/elements/1.1/identifier',
    )

    TARGET_MULTIMEDIA_EXTENSIONS = (
        'http://rs.tdwg.org/ac/terms/Multimedia',
        'http://rs.gbif.org/terms/1.0/Multimedia',
    )

    with zipfile.ZipFile(inzip, 'r') as dwca:
        # locate multimedia extensions, if any
        with dwca.open('meta.xml', 'r') as metaxmlbin:
            metaxml = io.TextIOWrapper(metaxmlbin, encoding='utf-8', errors='strict')
            metaxet = detxmlparse(metaxml)
            meta_xml_syntax_check(metaxet, f'{inzip}:meta.xml')
            metaxmlroot = metaxet.getroot()

            file_attributes = None
            file_locations = []
            for dwcacomponent in metaxmlroot:
                rowtype = dwcacomponent.get('rowType')
                if not rowtype.endswith('Multimedia'):
                    continue
                if rowtype not in TARGET_MULTIMEDIA_EXTENSIONS:
                    strout = f'{inzip}:meta.xml: warn: encountered unhandled Multimedia extension: rowType="{rowtype}'
                    print(strout, file=sys.stderr)
                    continue

                # found Multimedia extension, get CSV format information
                # and location within DwC-A .zip archive

                if file_attributes is not None or len(file_locations) > 0:
                    strout = f'{inzip}:meta.xml: warn: more than one declared Multimedia extension, using last one\n' + \
                        f'  saw: rowType="{rowtype}"\n' + \
                        f'  replacing: rowType="{file_attributes["rowType"]}"'
                    print(strout, file=sys.stderr)
                #TODO# consolidate, similar to in get_dwc_field {0b5a5137-4d88-4839-b9a0-12f3a3313876}
                file_attributes = copy.deepcopy(CSV_FORMAT_PARAMETERS_DEFAULT)
                file_attributes_unfiltered = dict(dwcacomponent.items())
                # remove attributes defined as a blank string
                for key in dwcacomponent.keys():
                    if dwcacomponent.get(key) == '':
                        file_attributes_unfiltered.pop(key)
                file_attributes.update(file_attributes_unfiltered)
                file_attributes['encoding'] = file_attributes['encoding'].lower()
                if file_attributes['encoding'] != 'utf-8':
                    raise NotImplementedError(f'{inzip}:meta.xml: extension for rowtype="{rowtype}" uses non-utf-8 file encoding -- {file_attributes["encoding"]}')
                # attributes are strings, but we ['ignoreHeaderLines'] MUST be int (for use later in this script)
                file_attributes['ignoreHeaderLines'] = int(file_attributes['ignoreHeaderLines'])

                file_locations_unfiltered = [location.text for location
                    in dwcacomponent.findall(f'{NS_DWCA}files/{NS_DWCA}location')]
                file_locations = [loc for loc in file_locations_unfiltered if loc] # removes blanks and None
                if len(file_locations) != len(file_locations_unfiltered):
                    strout = f'{inzip}:meta.xml: warn: Multimedia extension "{rowtype}" contains blank values'
                    print(strout, file=sys.stderr)

                field_matches = [(int(element.get('index')), element.get('term')) for element
                    in dwcacomponent.findall(f'{NS_DWCA}field')
                    if (element.get('term') in TARGET_MULTIMEDIA_TERMS and element.get('index') is not None)]
                field_matches.sort(key=lambda f: TARGET_MULTIMEDIA_TERMS.index(f[1]), reverse=False) # lower index is better
                file_attributes['targetColumns'] = field_matches

        # extract and write out URLs from file(s)
        for location in file_locations:
            try: # exceptions captured within for-loop to allow attempted processing of other files
                if urlparse(location).netloc:
                    strout = f'{inzip}:meta.xml: warn: ignoring unsupported Multimedia location -- {location}'
                    print(strout, file=sys.stderr)
                    continue
                # `location` *should be* a relative path into the DwC-A.zip archive...

                with dwca.open(location, 'r') as multimediabin:
                    multimediacsv = io.TextIOWrapper(multimediabin, file_attributes['encoding'])
                    mmedia_csvreader = csv.reader(multimediacsv, strict=strict, **dwca_file_attributes_to_csv_dialect_parameters(file_attributes))
                    ccsverror_consecutive = 0
                    for mediarecord in itertools.islice(
                            mmedia_csvreader,
                            file_attributes['ignoreHeaderLines'], # skip first N lines
                            None): # continue to end of file
                        try:
                            for i, coltype in file_attributes['targetColumns']:
                                strout = FMT_DUMP_MULTIMEDIA_URLS_OUTPUT % {'archive_path': inzip, 'type': coltype, 'url': mediarecord[i]} + '\n'
                                output.write(strout)
                                ccsverror_consecutive = 0
                        except csv.Error as exc:
                            ccsverror_consecutive += 1
                            strout = f'{inzip}:{location}:{mmedia_csvreader.line_num}: error#{ccsverror_consecutive}: bad csv line read\n' \
                                + traceback.format_exc()
                            # thin out error logs by only printing on the n-th occurrence
                            if ccsverror_consecutive in PRINT_ON_NTH_CONSECUTIVE_CSV_ERROR:
                                print(strout, file=sys.stderr)
                            if ccsverror_consecutive >= 20:
                                # but just stop processing if the file has too many errors
                                raise DwcaProcessingError(strout) from exc
            except Exception as exc:
                try: # added exception context
                    strerr = f'{inzip}:{location}: unhandled exception while processing file'
                    if isinstance(exc, TypeError):
                        strerr += f" (delimiter=\"{file_attributes['fieldsTerminatedBy']}\")"
                    raise DwcaProcessingError(strerr) from exc
                except Exception:
                    traceback.print_exc()

def get_dwc_field(inzip: FileDescriptorOrPath, output: TextIOWrapper, fields: str, strict = False):
    """(see argparse help at `parser_get_field` above)"""
    # validate parameters
    if not fields:
        raise ValueError("get_dwc_field() parameter 'field' is empty")
    fieldsjson: dict[str, list[str] | str] = json.loads(fields)
    for component in fieldsjson:
        if not isinstance(component, str):
            raise TypeError(f"get_dwc_field() parameter 'field', key '{str(component)} is not a string")
    
    class FieldLocationTarget(NamedTuple):
        inner_filepath: list[PathLike[str]]
        dwc_file_attributes_target: DwcaExtensionFormatAttributes
    field_location_targets: list[FieldLocationTarget] = []
    fieldspec_has_wildcard = '*' in fieldsjson.keys()

    with zipfile.ZipFile(inzip, 'r') as dwca:
        with dwca.open('meta.xml', 'r') as metaxmlbin:
            metaxml = io.TextIOWrapper(metaxmlbin, encoding='utf-8', errors='strict')
            metaxet = detxmlparse(metaxml)
            meta_xml_syntax_check(metaxet, f'{inzip}:meta.xml')
            metaxmlroot = metaxet.getroot()

            for dwcacomponent in metaxmlroot:
                rowtype = dwcacomponent.get('rowType')
                # skip processing for non-targeted rowTypes
                # if `fieldspec_has_wildcard` then *everything* has to be checked
                if not (fieldspec_has_wildcard or rowtype in fieldsjson.keys()):
                    continue

                #TODO# consolidate, similar to in dump_multimedia_urls {0b5a5137-4d88-4839-b9a0-12f3a3313876}
                file_attributes = copy.deepcopy(CSV_FORMAT_PARAMETERS_DEFAULT)
                # remove attributes defined as a blank string
                file_attributes.update({k:v for (k,v) in dwcacomponent.items() if v != ''})
                file_attributes['encoding'] = file_attributes['encoding'].lower()
                if file_attributes['encoding'] != 'utf-8':
                    raise NotImplementedError(f'{inzip}:meta.xml: extension for rowtype="{rowtype}" uses non-utf-8 file encoding -- {file_attributes["encoding"]}')
                # attributes are strings, but we ['ignoreHeaderLines'] MUST be int (for use later in this script)
                file_attributes['ignoreHeaderLines'] = int(file_attributes['ignoreHeaderLines'])

                file_locations_unfiltered = [location.text for location
                    in dwcacomponent.findall(f'{NS_DWCA}files/{NS_DWCA}location')]
                file_locations = [loc for loc in file_locations_unfiltered if loc] # removes blanks and None
                if len(file_locations) != len(file_locations_unfiltered):
                    strout = f'{inzip}:meta.xml: warn: extension "{rowtype}" contains blank values'
                    print(strout, file=sys.stderr)

                def is_term_in_fieldspec(term: str) -> bool:
                    for fsrowtype, fsterms in fieldsjson.items():
                        if fsrowtype not in ('*', rowtype):
                            continue
                        if isinstance(fsterms, str):
                            if term == fsterms:
                                return True
                        elif isinstance(fsterms, list):
                            if term in fsterms:
                                return True
                        else:
                            raise TypeError(f"invalid fieldspec: terms must be 'str' or 'list[str]', got: rowtype={fsrowtype}, type(terms)={type(fsterms)}")
                    return False # if we went through the whole fieldspec
                file_attributes['targetColumns'] = [(int(element.get('index')), element.get('term')) for element
                    in dwcacomponent.findall(f'{NS_DWCA}field')
                    if (is_term_in_fieldspec(element.get('term')) and element.get('index') is not None)]
                field_location_targets.append(FieldLocationTarget(file_locations, file_attributes))
            #end for dwcacomponent in metaxmlroot
        #end with metaxmlbin

        # extract and write out values from file(s)
        for filepaths, file_attributes in field_location_targets:
            for filepath in filepaths:
                try: # exceptions captured within for-loop to allow attempted processing of other files
                    if urlparse(filepath).netloc:
                        strout = f'{inzip}:meta.xml: warn: ignoring unsupported location -- {filepath}'
                        print(strout, file=sys.stderr)
                        continue
                    # `location` *should be* a relative path into the DwC-A.zip archive...

                    with dwca.open(filepath, 'r') as filebin:
                        filecsv = io.TextIOWrapper(filebin, file_attributes['encoding'])
                        csvreader = csv.reader(filecsv, strict=strict, **dwca_file_attributes_to_csv_dialect_parameters(file_attributes))
                        ccsverror_consecutive = 0
                        for record in itertools.islice(
                                csvreader,
                                file_attributes['ignoreHeaderLines'], # skip first N lines
                                None): # continue to end of file
                            try:
                                for i, coltype in file_attributes['targetColumns']:
                                    value_hivis = record[i]
                                    strout = FMT_DUMP_DWC_FIELD_OUTPUT % {
                                            'archive_path': inzip,
                                            'subfile': filepath,
                                            'rowtype': file_attributes['rowType'],
                                            'coltype': coltype,
                                            'value': (f'"{record[i]}"'
                                                if record[i]
                                                else "<empty>"),
                                        } + '\n'
                                    output.write(strout)
                                    ccsverror_consecutive = 0
                            except csv.Error as exc:
                                ccsverror_consecutive += 1
                                strout = f'{inzip}:{filepath}:{csvreader.line_num}: error#{ccsverror_consecutive}: bad csv line read\n' \
                                    + traceback.format_exc()
                                # thin out error logs by only printing on the n-th occurrence
                                if ccsverror_consecutive in PRINT_ON_NTH_CONSECUTIVE_CSV_ERROR:
                                    print(strout, file=sys.stderr)
                                if ccsverror_consecutive >= 20:
                                    # but just stop processing if the file has too many errors
                                    raise DwcaProcessingError(strout) from exc
                except Exception as exc:
                    try: # added exception context
                        strerr = f'{inzip}:{filepath}: unhandled exception while processing file'
                        if isinstance(exc, TypeError):
                            strerr += f' (delimiter="{file_attributes["fieldsTerminatedBy"]}")'
                        raise DwcaProcessingError(strerr) from exc
                    except Exception:
                        traceback.print_exc()

if __name__ == '__main__':
    sys.exit(main())
