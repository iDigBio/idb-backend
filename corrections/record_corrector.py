from . import *

import uuid
import copy

class RecordCorrector(object):

    def __init__(self):
        cursor = pg.cursor(str(uuid.uuid4()),cursor_factory=DictCursor)
        cursor.execute("select k::json,v::json from corrections")

        self.corrections = {}
        for r in cursor:
            uk = tuple(r["k"]["idigbio:data"].keys())
            uv = tuple([r["k"]["idigbio:data"][k] for k in uk])
            if uk not in self.corrections:
                self.corrections[uk] = {}

            self.corrections[uk][uv] = r["v"]

    def correct_record(self,d):
        corrected_dict = copy.deepcopy(d)
        corrected_keys = set()

        for c in self.corrections:
            l = []
            for k in c:
                if k in corrected_dict:
                    l.append(corrected_dict[k])
                else:
                    break
            else: # if we got to the end of the for without breaking
                uv = tuple(l)
                if uv in self.corrections[c]:
                    for k in self.corrections[c][uv].keys():
                        if k in corrected_dict:
                            if corrected_dict[k] != self.corrections[c][uv][k]:
                                corrected_dict["flag_" + k.replace(":","_").lower() + "_replaced"] = True
                                corrected_dict[k] = self.corrections[c][uv][k]
                                corrected_keys.add(k)
                            else:
                                # match
                                pass
                        else:
                            corrected_dict["flag_" + k.replace(":","_").lower() + "_added"] = True
                            corrected_dict[k] = self.corrections[c][uv][k]
                            corrected_keys.add(k)

        return (corrected_dict,corrected_keys)
