from __future__ import absolute_import
from idb.postgres_backend import apidbpool, NamedTupleCursor

import uuid
import copy
import traceback

from idb.helpers.etags import objectHasher


class RecordCorrector(object):
    corrections = None
    keytups = None

    def __init__(self, reload=True):
        if reload:
            self.reload()

    def reload(self):
        sql = "select k::json,v::json from corrections"
        self.keytups = set()

        self.corrections = {}
        for r in apidbpool.fetchiter(sql, name=str(uuid.uuid4()), cursor_factory=NamedTupleCursor):
            uk = tuple(r.k.keys())
            self.keytups.add(uk)

            etag = objectHasher("sha256", r.k)

            self.corrections[etag] = r.v

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

    def correct_record(self, d):
        if self.corrections is None:
            self.reload()

        corrected_dict = copy.deepcopy(d)
        corrected_keys = set()

        cd_keys = {k.lower(): k for k in corrected_dict.keys()}

        def get_etag(t):
            temp_d = {}
            for f in t:
                f_real = cd_keys.get(f, f)

                if f in d:
                    temp_d[f] = d[f].lower()
                elif f_real in d:
                    temp_d[f] = d[f_real].lower()
                elif f in corrected_dict:
                    temp_d[f] = corrected_dict[f].lower()
                elif f_real in corrected_dict:
                    temp_d[f] = corrected_dict[f_real].lower()
                else:
                    return None
            etag = objectHasher("sha256", temp_d)
            return etag

        for t in sorted(self.keytups, key=len):
            etag = get_etag(t)
            if etag in self.corrections:
                # Correct the record.
                for k in self.corrections[etag].keys():
                    if k == "dwc:scientificname":
                        continue

                    if k in cd_keys:
                        cdk = cd_keys[k]
                        if type(corrected_dict[cdk]) == list:
                            corrected_dict[cdk].extend(self.corrections[etag][k])
                        else:
                            if corrected_dict[cdk].lower() != self.corrections[etag][k]:
                                if self.corrections[etag][k] is None:
                                    corrected_dict["flag_" + k.replace(":","_").lower() + "_removed"] = True
                                else:
                                    corrected_dict["flag_" + k.replace(":","_").lower() + "_replaced"] = True
                                corrected_dict[cdk] = self.corrections[etag][k]
                                corrected_keys.add(cdk)
                            else:
                                # match
                                pass
                    else:
                        if not k.startswith("flag_"):
                            corrected_dict["flag_" + k.replace(":","_").lower() + "_added"] = True
                        corrected_dict[k] = self.corrections[etag][k]
                        corrected_keys.add(k)

        return (corrected_dict,corrected_keys)
