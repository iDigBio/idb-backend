from __future__ import division, absolute_import, print_function

import uuid
import zipfile

import pytest

from idigbio_workers.lib.download import generate_files
from idigbio_workers.lib.query_shim import queryFromShim

@pytest.mark.parametrize("form", ["csv", "tsv", "dwca-csv", "dwca-tsv"])
def test_generate_files(tmpdir, form):
    rq = {"genus": "acer", "stateprovince": "florida"}
    record_query = queryFromShim(rq, "records")["query"]
    mediarecord_query = None
    filename = generate_files(core_type="records",
                              core_source="indexterms",
                              form=form,
                              record_query=record_query, mediarecord_query=mediarecord_query,
                              filename=str(tmpdir / uuid.uuid4()))
    assert filename.startswith(str(tmpdir))
    if form.startswith('dwca'):
        ext = form.split('-')[1]
        zf = zipfile.ZipFile(filename, 'r')
        names = zf.namelist()
        assert 'meta.xml' in names
        assert 'records.citation.txt' in names
        assert 'occurrence.' + ext in names


@pytest.mark.parametrize("form", ["csv", "tsv", "dwca-csv", "dwca-tsv"])
def test_generate_files_no_results(tmpdir, form):
    rq = {"genus": "f9f90044-2604-4294-9d8c-3c827cc46330", "stateprovince": "florida"}
    record_query = queryFromShim(rq, "records")["query"]
    mediarecord_query = None
    filename = generate_files(core_type="records",
                              core_source="indexterms",
                              form=form,
                              record_query=record_query, mediarecord_query=mediarecord_query,
                              filename=str(tmpdir / uuid.uuid4()))
    assert filename.startswith(str(tmpdir))
    if form.startswith('dwca'):
        ext = form.split('-')[1]
        zf = zipfile.ZipFile(filename, 'r')
        names = zf.namelist()
        assert 'meta.xml' in names
        assert 'occurrence.' + ext in names
