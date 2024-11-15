from __future__ import absolute_import
from idb.postgres_backend import apidbpool, NamedTupleCursor

import uuid
import copy
import sys
import traceback

from idb.helpers.etags import objectHasher

if sys.version_info >= (3, 5):
    from typing import Dict, Set, Tuple, Optional
    from idb.helpers.types import (
        DwcTerm,
        DwcTermOrQualityFlag,
        DwcTermValue,
        QualityFlagValue,
        ETag,
        RecordData,
    )

protected_kingdoms = ["animalia", "plantae", "fungi", "chromista", "protista", "protozoa"]

class RecordCorrector(object):
    corrections = None # type: Dict[ETag, RecordData]
    keytups = None # type: Set[Tuple[DwcTermOrQualityFlag]]
    """Set of tuples of DwC terms present in corrections table;
    each tuple is an individual correction record _key_
    """

    def __init__(self, reload=True):
        """
        :param reload: If ``True``, will call :py:meth:`.reload()`.
            Otherwise, this instance will be created with no corrections information.
        """
        if reload:
            self.reload()
        else:
            self.corrections = {}
            self.keytups = set()


    def reload(self):
        """Loads database corrections table data into this instance"""
        sql = "select k::json,v::json from corrections"
        self.keytups = set()

        self.corrections = {}
        for r in apidbpool.fetchiter(sql, name=str(uuid.uuid4()), cursor_factory=NamedTupleCursor):
            uk = tuple(r.k.keys()) # type: Tuple[DwcTermOrQualityFlag]
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

                cur.execute("CREATE INDEX corrections_source ON corrections (source)")
                conn.commit()

    def correct_record(self, d):
        # type: (RecordData) -> Tuple[RecordData,  Set[DwcTermOrQualityFlag]]
        """
        :param d: Record data to correct
        :return: Tuple: 
            [0] = Corrected record data, 
            [1] = DwC terms replaced or added in corrected record data
        """
        corrected_dict = copy.deepcopy(d)
        corrected_keys = set()

        cd_keys = {k.lower(): k for k in corrected_dict.keys()}

        def get_etag(t):
            # type: (Tuple[DwcTermOrQualityFlag]) -> Optional[ETag]
            """Get ETag for supplied correction key ``t``.
            Returns ``None`` if any DwC term within ``t`` is not present
            in the original or correction-in-progress dict.
            """

            temp_d = {} # type: Dict[DwcTerm, DwcTermValue]
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

        #TODO#PERF# Are we re-sorting this thing every time we call this method?
        for t in sorted(self.keytups, key=len):
            etag = get_etag(t)
            #TODO#PERF# etag might be None. Should we short-circuit with a None check
            # rather than potentially checking if None is in self.corrections every time?
            # (It should never be.)
            if etag in self.corrections:
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
