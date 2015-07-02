import os
import sys
import traceback
import json
import requests
import uuid

mybase = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
if mybase not in sys.path:
    sys.path.append(mybase)

from ..lib.download import generate_files
from ..lib.query_shim import queryFromShim

#, getRecordsets
# from ..lib.mailer import send_mail
# from ..lib import storage

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

# def send_download_email(email,link,q,ip=None,source=None):
#     if email is not None and not email.endswith("@acis.ufl.edu"):
#         query = queryFromShim(q)
#         recordsets = getRecordsets("records",query)
#         stats_post = {
#             "type": "download",
#             "query": q,
#             "results": recordsets,
#             "recordtype": "records"
#         }
#         if ip is not None:
#             stats_post["ip"] = ip
#         if source is not None:
#             stats_post["source"] = source
#         s.post("http://idb-redis-stats.acis.ufl.edu:3000",data=json.dumps(stats_post), headers={'content-type': 'application/json'})
#     send_mail("data@idigbio.org",[email],"iDigBio Download Ready",mail_text.format(link,json.dumps(q)))

# def upload_download_file_to_ceph(tid):
#     s = storage.IDigBioStorage()

#     fkey = s.upload_file(tid + ".zip","idigbio-downloads",tid + ".zip")
#     fkey.set_metadata('Content-Type', 'application/zip')
#     os.unlink(tid + ".zip")
#     return "http://s.idigbio.org/idigbio-downloads/" + tid + ".zip"

@app.task(bind=True)
def downloader(self, params, email=None, ip=None, source=None):
    for rename in [("rq","record_query"),("mq","mediarecord_query")]:
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
    # link = upload_download_file_to_ceph(tid)
    # try:
    #     if email != None:
    #         send_download_email(email,link,q,ip=ip,source=source)
    # except:
    #     traceback.print_exc()
    return tid

def main():
    downloader({"stateprovince": "rhode island"},"godfoder@gmail.com",source="test",ip="128.227.150.136")

if __name__ == '__main__':
    main()
