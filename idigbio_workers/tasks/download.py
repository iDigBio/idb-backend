from __future__ import division, absolute_import, print_function

import datetime
import os
import json
import requests
import uuid

from path import tempdir, Path

from ..lib.download import generate_files, get_recordsets
from ..lib.query_shim import queryFromShim
from ..lib.mailer import send_mail

from idb.helpers.storage import IDigBioStorage
from celery.utils.log import get_task_logger
from celery.result import AsyncResult
#, getRecordsets

from .. import app


logger = get_task_logger('downloader')

mail_text = """
The download you requested from iDigBio is ready and can be retrieved from:

{0}

The query that produced this dataset was:

{1}

If you have any problems retrieving or using this file, please contact us at
data@idigbio.org, or by replying to this email.

Thank You,
The iDigBio Team
"""

s = requests.Session()



def upload_download_file_to_ceph(filename):
    s = IDigBioStorage()
    keyname, bucket = os.path.basename(filename), "idigbio-downloads"
    fkey = s.upload(s.get_key(keyname, bucket), filename, content_type='application/zip', public=True)
    return "https://s.idigbio.org/idigbio-downloads/" + fkey.name


def normalize_params(params):
    for rename in [("rq", "record_query"), ("mq", "mediarecord_query")]:
        if params[rename[0]] is not None:
            if rename[1].endswith("query"):
                params[rename[1]] = queryFromShim(params[rename[0]])["query"]
            else:
                params[rename[1]] = params[rename[0]]
        else:
            params[rename[1]] = params[rename[0]]
        del params[rename[0]]
    return params

@app.task(bind=True)
def downloader(self, params):
    tid = self.request.id or str(uuid.uuid4())
    logger.info("Kicking off downloader: %s", tid)
    params = normalize_params(params)
    # hid = objectHasher("sha1", params, sort_arrays=True, sort_keys=True)
    # filename = hid + '-' + datetime.datetime.now().isoformat
    filename = tid

    with tempdir() as td:
        filename = td / filename
        tid = generate_files(filename=filename, **params)
        logger.debug("Finished generating file, uploading to ceph, size: %s", Path(tid).getsize())
        link = upload_download_file_to_ceph(tid)
        logger.debug("Finished uploading to ceph")
    return link

@app.task(bind=True, ignore_result=True, max_retries=None)
def blocker(self, rid, pollbase=1.25):
    """Wait for an AsyncResult to be ready, then return its result.

    This can be used to append tasks to a running one, block on the
    running and chain after this.

    """
    ar = AsyncResult(rid)
    if ar.ready():
        return ar.result
    else:
        raise self.retry(kwargs={'pollbase': pollbase},
                         countdown=pollbase ** self.request.retries)


@app.task(ignore_result=True)
def send_download_email(link, email, params, ip=None, source=None):
    if email is None:
        logger.warn("send email called with no email")
        return
    logger.info("Sending email to %s with link %s", email, link)
    if not email.endswith("@acis.ufl.edu"):
        q, recordsets = get_recordsets(params)
        stats_post = {
            "type": "download",
            "query": params["rq"] or q,
            "results": recordsets,
            "recordtype": "records"
        }
        if ip is not None:
            stats_post["ip"] = ip
        if source is not None:
            stats_post["source"] = source
        s.post("http://idb-portal-telemetry-collector.acis.ufl.edu:3000",
               data=json.dumps(stats_post), headers={'content-type': 'application/json'})
    send_mail("data@idigbio.org", [email], "iDigBio Download Ready",
              mail_text.format(link, json.dumps(params)))


def main():
    downloader({
        "core_source": "indexterms",
        "core_type": "records",
        "form": "dwca-csv",
        "mediarecord_fields": None,
        "mq": None,
        "record_fields": None,
        "rq": {
            "genus": "acer"
        }
    }, "godfoder@gmail.com", source="test", ip="128.227.150.136")


if __name__ == '__main__':
    main()
