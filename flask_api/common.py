import requests
import traceback

from .config import RIAK_URL

s = requests.Session()

def load_data_from_riak(t, u, e):
    if RIAK_URL is not None:
        try:
            print RIAK_URL.format(t,u,e)
            resp = s.get(RIAK_URL.format(t,u,e))
            resp.raise_for_status()
            return resp.json()["idigbio:data"]
        except:
            traceback.print_exc()
            return None
    else:
        return None