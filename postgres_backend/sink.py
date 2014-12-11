from . import *

import json

class PostgresRecordSink(object):

    def __init__(self,check_first=True,skip_existing=False):
        self.cursor = pg.cursor()
        self.check_first = check_first
        self.skip_existing = skip_existing

    def __enter__(self):
        self.cursor.execute("BEGIN")
        return self.cursor

    def commit(self):
        pg.commit()

    def __exit__(self, type, value, traceback):
        self.commit()

    def set_record_value(self,rv,tcursor=None):
        assert "uuid" in rv
        assert "etag" in rv
        assert "type" in rv
        assert "data" in rv

        cursor = self.cursor
        if tcursor is not None:            
            cursor = tcursor

        update = False
        if self.check_first:
            cursor.execute("SELECT id FROM cache WHERE id=%s", (rv["uuid"],))
            if cursor.fetchone() is not None:
                update = True

        if update and self.skip_existing:
            return

        if update:
            cursor.execute("UPDATE cache SET id=%s, type=%s, etag=%s, data=%s, updated_at=now() WHERE id=%s", (rv["uuid"],rv["type"],rv["etag"],json.dumps(rv["data"]),rv["uuid"]))
        else:
            cursor.execute("INSERT INTO cache (id,type,etag,data) VALUES (%s,%s,%s,%s)", (rv["uuid"],rv["type"],rv["etag"],json.dumps(rv["data"])))


def main():
    prs = PostgresRecordSink()

    with prs as tcursor:
        prs.set_record_value({
          "etag": "87d9ac82055dc115aa2f5cf79b4cf3a6b85169e0",
          "type": "mediarecords",
          "data": {
            "idigbio:version": 0, 
            "idigbio:createdBy": "872733a2-67a3-4c54-aa76-862735a5f334", 
            "idigbio:links": {
              "owner": [
                "872733a2-67a3-4c54-aa76-862735a5f334"
              ], 
              "record": [
                "http://api.idigbio.org/v1/records/f6afcecb-a0dc-417d-b80e-c87412b9b724"
              ], 
              "recordset": [
                "http://api.idigbio.org/v1/recordsets/e812c193-89b5-4da8-980d-e759ac50ffba"
              ]
            }, 
            "idigbio:uuid": "e613dd12-7f41-4c4d-a7e0-ab831f3ad437", 
            "idigbio:data": {
              "dcterms:type": "StillImage", 
              "xmpRights:WebStatement": "http://creativecommons.org/licenses/by-nc-sa/3.0/", 
              "ac:subtype": "Photograph", 
              "ac:metadataLanguage": "en", 
              "xmpRights:UsageTerms": "CC BY-NC-SA (Attribution-NonCommercial-ShareAlike)", 
              "dcterms:format": "image/jpeg", 
              "coreid": "2798292", 
              "xmpRights:Owner": "Missouri Botanical Garden (MO)", 
              "ac:providerManagedID": "urn:uuid:bfb52795-45fb-45de-a3b7-d3df56337efa", 
              "ac:accessURI": "http://storage.idigbio.org/mo/bryophytes/MO-2630/MO-2630100_lg.jpg", 
              "xmp:MetadataDate": "2014-05-15 01:38:22", 
              "ac:associatedSpecimenReference": "http://bryophyteportal.org/portal/collections/individual/index.php?occid=2798292"
            }, 
            "idigbio:etag": "87d9ac82055dc115aa2f5cf79b4cf3a6b85169e0", 
            "idigbio:recordIds": [
              "urn:uuid:bfb52795-45fb-45de-a3b7-d3df56337efa"
            ], 
            "idigbio:dateModified": "2014-07-04T20:20:54.967Z"
          }, 
          "uuid": "e613dd12-7f41-4c4d-a7e0-ab831f3ad437"
        },tcursor=tcursor)

if __name__ == '__main__':
    main()