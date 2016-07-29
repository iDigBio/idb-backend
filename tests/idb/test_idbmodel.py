from __future__ import division, absolute_import, print_function
import pytest

@pytest.mark.xfail
def test_insert_and_delete(logger, testidbmodel):
    test_uuid = "00000000-0000-0000-0000-000000000000"
    testidbmodel.set_record(test_uuid, 'record', None, {}, [], [])
    testidbmodel.delete_item(test_uuid)


    ### The following is some test code that used to be at the bottom of `postgres_backend/db.py`; it doesn't really work well but is perhaps a guide towards helping to build better tests here.
    # if os.environ["ENV"] == "test":
    #     import requests
    #     ses = requests.Session()

    #     print("Creating test schema")
    #     db = PostgresDB()
    #     db.drop_schema()
    #     db.create_schema()

    #     r = ses.get("http://api.idigbio.org/v1/records/")
    #     r.raise_for_status()
    #     ro = r.json()

    #     reccount = 0
    #     mediarecords = set()
    #     for rec in ro["idigbio:items"]:
    #         print "record", rec["idigbio:uuid"]
    #         rr = ses.get(
    #             "http://api.idigbio.org/v1/records/{0}".format(rec["idigbio:uuid"]))
    #         rr.raise_for_status()
    #         rro = rr.json()
    #         mrs = []
    #         if "mediarecord" in rro["idigbio:links"]:
    #             mrs = [s.split("/")[-1]
    #                    for s in rro["idigbio:links"]["mediarecord"]]
    #         mediarecords.update(mrs)
    #         db.set_record(
    #             rro["idigbio:uuid"],
    #             "record",
    #             rro["idigbio:links"]["recordset"][0].split("/")[-1],
    #             rro["idigbio:data"],
    #             rro["idigbio:recordIds"],
    #             []
    #         )
    #         reccount += 1

    #     for mrid in mediarecords:
    #         print "mediarecord", mrid
    #         rr = ses.get(
    #             "http://api.idigbio.org/v1/mediarecords/{0}".format(mrid))
    #         rr.raise_for_status()
    #         rro = rr.json()
    #         recs = [s.split("/")[-1] for s in rro["idigbio:links"]["record"]]
    #         mediarecords.update(mrs)
    #         db.set_record(
    #             rro["idigbio:uuid"],
    #             "mediarecord",
    #             rro["idigbio:links"]["recordset"][0].split("/")[-1],
    #             rro["idigbio:data"],
    #             rro["idigbio:recordIds"],
    #             recs
    #         )

    #     db.commit()
    #     print "Imported ", reccount, "records and ", len(mediarecords), "mediarecords."
    # else:
    #     print "ENV not test, refusing to run"
