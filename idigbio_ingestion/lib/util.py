import requests
import shutil
import traceback

s = requests.Session()

def download_file(url,fname,params={},timeout=10):
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/117.0'
            # You can add other headers here if needed, like Accept, Accept-Language, etc.
        }
        r = s.get(url, params=params, stream=True, timeout=timeout, headers=headers)
        r.raise_for_status()
        with open(fname, 'wb') as f:
            r.raw.decode_content = True
            shutil.copyfileobj(r.raw, f)
            return True
    except:
        traceback.print_exc()
        return False
