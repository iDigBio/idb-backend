from __future__ import absolute_import
from idb.postgres_backend import apidbpool, NamedTupleCursor

import uuid
import copy
import traceback

from idb.helpers.etags import objectHasher

protected_kingdoms = ["animalia", "plantae", "fungi", "chromista", "protista", "protozoa"]

class RecordCorrector(object):
    corrections = None
    keytups = None

    def __init__(self, reload=True):
        if reload:
            self.reload()
        else:
            self.corrections = {}
            self.keytups = set()


    def reload(self):
        sql = "select k::json,v::json,source from corrections"
        self.keytups = set()

        self.corrections = {}
        for r in apidbpool.fetchiter(sql, name=str(uuid.uuid4()), cursor_factory=NamedTupleCursor):
            uk = tuple(r.k.keys())
            self.keytups.add(uk)

            etag = objectHasher("sha256", r.k)

            self.corrections[etag] = (r.v,r.source)

    def create_schema(self):
        with apidbpool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(""" CREATE TABLE IF NOT EXISTS corrections (
                        id bigserial PRIMARY KEY,
                        k jsonb NOT NULL,
                        v jsonb NOT NULL,
                        approved boolean NOT NULL DEFAULT false,
                        source varchar(50) NOT NULL,
                        updated_at timestamp DEFAULT now()
                    )
                """)
                cur.execute(""" CREATE TABLE IF NOT EXISTS annotations (
                        id bigserial PRIMARY KEY,
                        uuids_id uuid NOT NULL REFERENCES uuids(id),
                        v jsonb NOT NULL,
                        approved boolean NOT NULL DEFAULT false,
                        source varchar(50) NOT NULL,
                        updated_at timestamp DEFAULT now()
                    )
                """)
                conn.commit()
                try:
                    cur.execute("CREATE INDEX corrections_source ON corrections (source)")
                    conn.commit()
                except:
                    pass

    def get_correction_list(self,d):
        if self.corrections is None:
            self.reload()

        corrected_dict = copy.deepcopy(d)
        corrected_keys = set()

        cd_keys = {k.lower(): k for k in corrected_dict.keys()}

        correction_list = []

        for t in self.keytups:
            d = {}
            for f in t:
                f_real = cd_keys.get(f, f)

                if f in d:
                    d[f] = d[f].lower()
                elif f_real in d:
                    d[f] = d[f_real].lower()
                elif f in corrected_dict:
                    d[f] = corrected_dict[f].lower()
                elif f_real in corrected_dict:
                    d[f] = corrected_dict[f_real].lower()
                else:
                    break
            else:  # if we got to the end of the for without breaking
                etag = objectHasher("sha256", d)
                if etag in self.corrections:
                    d.update(self.corrections[etag][0])
                    correction_list.append((d, self.corrections[etag][1]))

        return correction_list

    def correct_record(self,d,correction_list=None):
        if self.corrections is None:
            self.reload()

        corrected_dict = copy.deepcopy(d)
        corrected_keys = set()

        cd_keys = dict([(k.lower(),k) for k in corrected_dict.keys()])

        if correction_list is None:
            correction_list = self.get_correction_list()

        for correction, source in correction_list:
            for k in correction.keys():
                if k == "dwc:scientificname":
                    continue

                if k in cd_keys:
                    cdk = cd_keys[k]
                    if type(corrected_dict[cdk]) == list:
                        corrected_dict[cdk].extend(correction[k])
                    else:
                        if corrected_dict[cdk].lower() != correction[k]:
                            if correction[k] is None:
                                corrected_dict["flag_" + k.replace(":","_").lower() + "_removed"] = True
                            else:
                                corrected_dict["flag_" + k.replace(":","_").lower() + "_replaced"] = True
                            corrected_dict[cdk] = correction[k]
                            corrected_keys.add(cdk)
                        else:
                            # match
                            pass
                else:
                    if not k.startswith("flag_"):
                        corrected_dict["flag_" + k.replace(":","_").lower() + "_added"] = True
                    corrected_dict[k] = correction[k]
                    corrected_keys.add(k)

        return (corrected_dict,corrected_keys)
