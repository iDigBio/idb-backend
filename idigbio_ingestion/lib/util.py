import requests
import shutil

s = requests.Session()

def download_file(url,fname,params={}):
    r = s.get(url, params=params, stream=True)
    r.raise_for_status()
    with open(fname, 'wb') as f:
        r.raw.decode_content = True
        shutil.copyfileobj(r.raw, f)
        return True