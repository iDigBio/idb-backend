from . import *

import uuid
import copy

class RecordCorrector(object):

    def __init__(self):
        self.reload()

    def reload(self):
        cursor = pg.cursor(str(uuid.uuid4()),cursor_factory=DictCursor)
        cursor.execute("select k::json,v::json from corrections")

        self.corrections = {}
        for r in cursor:
            uk = tuple(r["k"].keys())
            uv = tuple([r["k"][k] for k in uk])
            if uk not in self.corrections:
                self.corrections[uk] = {}

            self.corrections[uk][uv] = r["v"]
        pg.rollback()

    def create_schema(self):
        cur = pg.cursor(cursor_factory=DictCursor)
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
        pg.commit()
        try:
            cur.execute("CREATE INDEX corrections_source ON corrections (source)")
            pg.commit()
        except:
            pg.rollback()

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
