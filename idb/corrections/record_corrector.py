from __future__ import absolute_import
from idb.postgres_backend import apidbpool, DictCursor

import uuid
import copy


class RecordCorrector(object):
    def __init__(self):
        self.reload()

    def reload(self):
        sql = "select k::json,v::json from corrections where source = ANY('{\"data_dictionaries_2\",\"data_dictionaries_1\",\"gbif_checklist\"}')"

        self.corrections = {}
        for r in apidbpool.fetchiter(sql, name=str(uuid.uuid4()), cursor_factory=DictCursor):
            uk = tuple(r["k"].keys())
            uv = tuple([r["k"][k] for k in uk])
            if uk not in self.corrections:
                self.corrections[uk] = {}

            self.corrections[uk][uv] = r["v"]

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

    def correct_record(self,d):
        corrected_dict = copy.deepcopy(d)
        corrected_keys = set()

        cd_keys = dict([(k.lower(),k) for k in corrected_dict.keys()])

        for c in self.corrections:
            l = []
            for k in c:
                if k in cd_keys:
                    l.append(corrected_dict[cd_keys[k]].lower())
                else:
                    break
            else: # if we got to the end of the for without breaking
                uv = tuple(l)
                if uv in self.corrections[c]:
                    for k in self.corrections[c][uv].keys():
                        if k in cd_keys:
                            if corrected_dict[cd_keys[k]].lower() != self.corrections[c][uv][k]:
                                if self.corrections[c][uv][k] is None:
                                    corrected_dict["flag_" + k.replace(":","_").lower() + "_removed"] = True
                                else:
                                    corrected_dict["flag_" + k.replace(":","_").lower() + "_replaced"] = True
                                corrected_dict[cd_keys[k]] = self.corrections[c][uv][k]
                                corrected_keys.add(cd_keys[k])
                            else:
                                # match
                                pass
                        else:
                            corrected_dict["flag_" + k.replace(":","_").lower() + "_added"] = True
                            corrected_dict[k] = self.corrections[c][uv][k]
                            corrected_keys.add(k)

        return (corrected_dict,corrected_keys)
