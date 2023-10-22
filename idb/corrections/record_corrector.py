from __future__ import absolute_import
from idb.postgres_backend import apidbpool, NamedTupleCursor

import json
import uuid
import copy
import subprocess
import traceback
import os

from idb.helpers.etags import objectHasher

protected_kingdoms = ["animalia", "plantae", "fungi", "chromista", "protista", "protozoa"]

class RecordCorrector(object):
    corrections = None
    corrections_done = False
    corrections_file_written = False
    corrections_file = None
    keytups = None
    window_size = 3200 * 1024 * 1024  # 3200MB

    def __init__(self, reload=True):
        if reload:
            self.reload()
            self.corrections_done = True
            print ("keytups length: " + str(len(self.keytups)))
        else:
            self.corrections = {}
            self.corrections_file = None
            self.keytups = set()

    def create_corrections_file(self):
        # Create the corrections file in the current directory
        with open("collectionsKV.json", "w") as f:
           pass  # This creates an empty fil

    def corrections_etag(self, etag):
        if self.corrections_file_written is True:
            return self.read_corrections_etag(etag)
        return {}
    
    def read_corrections_etag(self, target_etag):
        rg_command = ['rg', '-N', '--no-messages', target_etag, 'collectionsKV.json']

        try:
            process = subprocess.Popen(rg_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            output, error = process.communicate()

            if process.returncode == 0:
                if output:
                    # Split the output into lines
                    lines = output.strip().split('\n')

                    # Initialize an empty dictionary to store the merged JSON objects
                    merged_json = {}

                    for line in lines:
                        # Parse each line as a JSON object
                        json_data = json.loads(line)

                        # Merge the JSON object into the dictionary
                        for key, value in json_data.items():
                            merged_json[key] = value

                    return merged_json
                else:
                    return None
            elif process.returncode == 1:
                # No match found
                return None
            else:
                print("Error running ripgrep: {0}".format(error))
        except Exception as e:
            print("Error: {0}".format(e))
            
    def reload(self):
        
        #todo: Check if the corrections file exists, and if it doesn't, create it
        """ if not os.path.exists("collectionsKV.json"): """
        sql = "select k::json,v::json from corrections"
        self.keytups = set()
        self.create_corrections_file()
        # Open the corrections file in write mode to populate it
        with open("collectionsKV.json", "w") as corrections_file:
            for r in apidbpool.fetchiter(sql, name=str(uuid.uuid4()), cursor_factory=NamedTupleCursor):
                uk = tuple(r.k.keys())
                self.keytups.add(uk)

                etag = objectHasher("sha256", r.k)

                # Write the data to the corrections file
                corrections_file.write(json.dumps({etag: r.v}) + '\n')
                self.corrections_file_written = True
        
        #todo: cache self.keytups

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
            if etag is not None:
                self.corrections = self.corrections_etag(etag)
                if self.corrections is not None:
                # Correct the record.

                # If a correction would have replaced one of the protected kingdom values,
                # apply a flag instead.
                # print(t, self.corrections[etag])   # consider adding logging / debug lines instead

                
                    if (
                        "dwc:kingdom" in self.corrections[etag] and
                        "dwc:kingdom" in corrected_dict and
                        corrected_dict["dwc:kingdom"].lower() != self.corrections[etag]["dwc:kingdom"] and
                        corrected_dict["dwc:kingdom"].lower() in protected_kingdoms
                    ):
                            corrected_dict["flag_dwc_kingdom_suspect"] = True
                            continue

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
