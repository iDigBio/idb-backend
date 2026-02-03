from __future__ import division, absolute_import, print_function

import datetime
import os
import json
import requests
import uuid

from pathlib import Path
import tempfile

# Create temporary directory
tmpdir = tempfile.mkdtemp()
temp_path = Path(tmpdir)

from ..lib.download import generate_files, get_recordsets
from ..lib.query_shim import queryFromShim
from ..lib.mailer import send_mail

from idb.helpers.storage import IDigBioStorage
from celery.utils.log import get_task_logger
from celery.result import AsyncResult
#, getRecordsets

from idigbio_workers import app


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
    storage = IDigBioStorage()
    bucket = "idigbio-downloads"
    keyname = os.path.basename(filename)

    obj = storage.get_key(keyname, bucket)
    uploaded = storage.upload(obj, filename, content_type="application/zip", public=True)

    # boto3 s3.Object => .key
    obj_key = (
        getattr(uploaded, "key", None)
        or getattr(obj, "key", None)
        or getattr(uploaded, "name", None)   # backwards compat if wrapper returns boto2-ish
        or getattr(obj, "name", None)
        or keyname
    )

    return f"https://s.idigbio.org/{bucket}/{obj_key}"


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
    filename = tid
    
    with tempfile.TemporaryDirectory() as td:
        td_path = Path(td)
        filename = str(td_path / filename)  # Convert back to string
        tid = generate_files(filename=filename, **params)
        logger.debug("Finished generating file, uploading to ceph, size: %s", Path(tid).stat().st_size)
        link = upload_download_file_to_ceph(tid)
        logger.debug("Finished uploading to ceph")
    return link


@app.task(bind=True, ignore_result=True, max_retries=None)
def blocker(self, rid, pollbase=1.25):
    ar = AsyncResult(rid)

    if not ar.ready():
        raise self.retry(
            kwargs={"pollbase": pollbase},
            countdown=pollbase ** self.request.retries,
        )

    # At this point it's done; use state/result (no .get()).
    if ar.successful():
        return ar.result  # should be your link (JSON-serializable)

    # FAILURE (or other terminal states): do NOT return ar.result (it's often an exception)
    exc = ar.result
    tb = ar.traceback

    if isinstance(exc, BaseException):
        raise RuntimeError(f"Upstream task {rid} failed: {exc}\n{tb}") from exc

    raise RuntimeError(f"Upstream task {rid} finished in state={ar.state}: {exc}\n{tb}")


@app.task(ignore_result=True)
def send_download_email(link, email, params, ip=None, source=None):
    if email is None:
        logger.warn("send email called with no email")
        return
    logger.info("Sending email to %s with link %s", email, link)
    if not email.endswith("@acis.ufl.edu"):
        q, recordsets = get_recordsets(params)
        record_query = params.get("rq") or params.get("record_query") or q
        stats_post = {
            "type": "download",
            "query": record_query,
            "results": recordsets,
            "recordtype": "records"
        }
        if ip is not None:
            stats_post["ip"] = ip
        if source is not None:
            stats_post["source"] = source
        s.post("http://10.13.45.190:3000",
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
