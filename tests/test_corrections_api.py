import unittest

import os
import sys
import base64
import json

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))))

print sys.path

from corrections.api import app

class TestAnnotationsAPI(unittest.TestCase):
    def setUp(self):
        app.config["TESTING"] = True
        self.app = app.test_client()

    def tearDown(self):
        cur = app.config["DB"].cursor()
        cur.execute("DELETE FROM annotations WHERE source='00000000-0000-0000-0000-000000000000'")
        app.config["DB"].commit()

    def getAuthHeader(self):
        return {
            'Authorization': 'Basic ' + base64.b64encode("00000000-0000-0000-0000-000000000000" + \
            ":" + "testauthnotarealapikey")
        }

    def test_get_annotations(self):
        resp = self.app.get("/v2/annotations")
        self.assertEqual(resp.status_code, 200)

    def test_post_annotation_empty(self):
        resp = self.app.post("/v2/annotations", data=json.dumps({}), content_type="application/json", headers=self.getAuthHeader())
        self.assertEqual(resp.status_code, 400)

    def test_post_annotation_uuid_only(self):
        resp = self.app.post("/v2/annotations", data=json.dumps({"uuid": "0000012b-9bb8-42f4-ad3b-c958cb22ae45"}), content_type="application/json", headers=self.getAuthHeader())
        self.assertEqual(resp.status_code, 400)

    def test_post_annotation_values_only(self):
        resp = self.app.post("/v2/annotations", data=json.dumps({"values": {"blah": "blah"}}), content_type="application/json", headers=self.getAuthHeader())
        self.assertEqual(resp.status_code, 400)

    def test_post_annotation_empty_values(self):
        resp = self.app.post("/v2/annotations", data=json.dumps({"uuid": "0000012b-9bb8-42f4-ad3b-c958cb22ae45", "values": {}}), content_type="application/json", headers=self.getAuthHeader())
        self.assertEqual(resp.status_code, 400)

    def test_post_annotation_bad_uuid(self):
        resp = self.app.post("/v2/annotations", data=json.dumps({"uuid": "00000000-0000-0000-0000-000000000000", "values": {"blah": "blah"}}), content_type="application/json", headers=self.getAuthHeader())
        self.assertEqual(resp.status_code, 400)

    def test_post_annotation_success(self):
        resp = self.app.post("/v2/annotations", data=json.dumps({"uuid": "0000012b-9bb8-42f4-ad3b-c958cb22ae45", "values": {"blah": "blah"}}), content_type="application/json", headers=self.getAuthHeader())
        self.assertEqual(resp.status_code, 200)
        obj = json.loads(resp.data)
        assert obj["approved"] == False

    def test_get_annotation(self):
        resp = self.app.post("/v2/annotations", data=json.dumps({"uuid": "0000012b-9bb8-42f4-ad3b-c958cb22ae45", "values": {"blah": "blah"}}), content_type="application/json", headers=self.getAuthHeader())
        self.assertEqual(resp.status_code, 200)
        obj = json.loads(resp.data)
        resp = self.app.get("/v2/annotations/" + repr(obj["id"]))
        self.assertEqual(resp.status_code, 200)

class TestCorrectionsAPI(unittest.TestCase):
    def setUp(self):
        app.config["TESTING"] = True
        self.app = app.test_client()

    def tearDown(self):
        cur = app.config["DB"].cursor()
        cur.execute("DELETE FROM corrections WHERE source='00000000-0000-0000-0000-000000000000'")
        app.config["DB"].commit()

    def getAuthHeader(self):
        return {
            'Authorization': 'Basic ' + base64.b64encode("00000000-0000-0000-0000-000000000000" + \
            ":" + "testauthnotarealapikey")
        }

    def test_get_corrections(self):
        resp = self.app.get("/v2/annotations")
        self.assertEqual(resp.status_code, 200)

    def test_post_correction_empty(self):
        resp = self.app.post("/v2/corrections", data=json.dumps({}), content_type="application/json", headers=self.getAuthHeader())
        self.assertEqual(resp.status_code, 400)

    def test_post_correction_keys_only(self):
        resp = self.app.post("/v2/corrections", data=json.dumps({"keys": {"blah": "blah"}}), content_type="application/json", headers=self.getAuthHeader())
        self.assertEqual(resp.status_code, 400)

    def test_post_correction_values_only(self):
        resp = self.app.post("/v2/corrections", data=json.dumps({"values": {"blah": "blah"}}), content_type="application/json", headers=self.getAuthHeader())
        self.assertEqual(resp.status_code, 400)

    def test_post_correction_empty_keys(self):
        resp = self.app.post("/v2/corrections", data=json.dumps({"keys": {}, "values": {"blah": "blah"}}), content_type="application/json", headers=self.getAuthHeader())
        self.assertEqual(resp.status_code, 400)

    def test_post_correction_empty_values(self):
        resp = self.app.post("/v2/corrections", data=json.dumps({"keys": {"blah": "blah"}, "values": {}}), content_type="application/json", headers=self.getAuthHeader())
        self.assertEqual(resp.status_code, 400)

    def test_post_correction_success(self):
        resp = self.app.post("/v2/corrections", data=json.dumps({"keys": {"blah": "blah"}, "values": {"blah": "blah"}}), content_type="application/json", headers=self.getAuthHeader())
        self.assertEqual(resp.status_code, 200)
        obj = json.loads(resp.data)
        assert obj["approved"] == False

    def test_get_correction(self):
        resp = self.app.post("/v2/corrections", data=json.dumps({"keys": {"blah": "blah"}, "values": {"blah": "blah"}}), content_type="application/json", headers=self.getAuthHeader())
        self.assertEqual(resp.status_code, 200)
        obj = json.loads(resp.data)
        resp = self.app.get("/v2/corrections/" + repr(obj["id"]))
        self.assertEqual(resp.status_code, 200)


if __name__ == '__main__':
    unittest.main()
