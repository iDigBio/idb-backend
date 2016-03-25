from . import *

import uuid
import copy
import traceback

from idb.helpers.etags import objectHasher

class RecordCorrector(object):

    def __init__(self):
        self.reload()

    def reload(self):
        self.keytups = set()


        cursor = pg.cursor(str(uuid.uuid4()),cursor_factory=DictCursor)
        cursor.execute("select k::json, v::json from corrections")

        self.corrections = {}
        for r in cursor:
            try:
                uk = tuple(r["k"].keys())
                self.keytups.add(uk)

                etag = objectHasher("sha256", r["k"])

                self.corrections[etag] = r["v"]
            except:
                traceback.print_exc()
                print r, [type(f) for f in r]
                raise Exception

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

        for t in self.keytups:
            d = {}
            for f in t:
                f_real = f
                if f in cd_keys:
                    f_real = cd_keys[f]

                if f in corrected_dict:
                    d[f] = corrected_dict[f].lower()
                elif f_real in corrected_dict:
                    d[f] = corrected_dict[f_real].lower()
                else:
                    break
            else: # if we got to the end of the for without breaking
                etag = objectHasher("sha256", d)
                if etag in self.corrections:
                    for k in self.corrections[etag].keys():
                        if k == "dwc:scientificname":
                            continue

                        if k in cd_keys:
                            cdk = cd_keys[k]
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
                            corrected_dict["flag_" + k.replace(":","_").lower() + "_added"] = True
                            corrected_dict[k] = self.corrections[etag][k]
                            corrected_keys.add(k)

        return (corrected_dict,corrected_keys)
