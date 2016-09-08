import os
import sys
import traceback
import json
import requests
import uuid

from ..lib.download import generate_files, get_recordsets, generate_queries
from ..lib.query_shim import queryFromShim
from ..lib.mailer import send_mail

from idb.helpers.storage import IDigBioStorage

#, getRecordsets

from .. import app


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

def send_download_email(email, link, params, ip=None, source=None):
    if email is not None and not email.endswith("@acis.ufl.edu"):
        q, recordsets = get_recordsets(params)
        stats_post = {
            "type": "download",
            "query": q,
            "results": recordsets,
            "recordtype": "records"
        }
        if ip is not None:
            stats_post["ip"] = ip
        if source is not None:
            stats_post["source"] = source
        s.post("http://idb-redis-stats.acis.ufl.edu:3000",
               data=json.dumps(stats_post), headers={'content-type': 'application/json'})
    send_mail("data@idigbio.org", [email], "iDigBio Download Ready",
              mail_text.format(link, json.dumps(params)))


def upload_download_file_to_ceph(tid):
    s = IDigBioStorage()
    fkey = s.upload_file(tid, "idigbio-downloads", tid)
    fkey.set_metadata('Content-Type', 'application/zip')
    fkey.make_public()
    os.unlink(tid)
    return "http://s.idigbio.org/idigbio-downloads/" + tid


@app.task(bind=True)
def downloader(self, params, email=None, ip=None, source=None):
    original_params = {}
    original_params.update(params)
    for rename in [("rq", "record_query"), ("mq", "mediarecord_query")]:
        if params[rename[0]] is not None:
            if rename[1].endswith("query"):
                params[rename[1]] = queryFromShim(params[rename[0]])["query"]
            else:
                params[rename[1]] = params[rename[0]]
        else:
            params[rename[1]] = params[rename[0]]
        del params[rename[0]]

    if self.request.id is not None:
        params["filename"] = self.request.id
    else:
        params["filename"] = str(uuid.uuid4())
    tid = generate_files(**params)[0]
    link = upload_download_file_to_ceph(tid)
    try:
        if email is not None:
            send_download_email(email, link, original_params, ip=ip, source=source)
    except:
        traceback.print_exc()
    return link


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
