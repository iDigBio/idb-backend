import os
import sys
import traceback

from idb.helpers.storage import IDigBioStorage

import PIL.Image

import cStringIO

from collections import Counter

from idb.postgres_backend.db import PostgresDB

THUMBNAIL_WIDTH = 260
WEBVIEW_WIDTH = 600

s = IDigBioStorage()
db = PostgresDB()

def img_from_key(key):
    try:
        buff = cStringIO.StringIO()
        key.get_contents_to_file(buff)
        buff.seek(0)
        img = PIL.Image.open(buff).convert("RGB")
        return img
    except Exception as e:
        raise Exception(traceback.format_exc() +"Exception during processing of "+key.name)
        
    
def save_to_buffer(img, **kwargs):
    dervbuff = cStringIO.StringIO()
    img.save(dervbuff,**kwargs)
    dervbuff.seek(0)
    return dervbuff

def generate_derivative(img, derivative_width):
    if img.size[0] > derivative_width:
        derivative_width_percent = (derivative_width / float(img.size[0]))
        derivative_horizontal_size = int((float(img.size[1]) * float(derivative_width_percent)))
        derv = img.resize((derivative_width, derivative_horizontal_size), PIL.Image.BILINEAR)
        return derv
    else:
        return img
        
def check_and_generate(name,env="prod"):
    tkey = s.get_key(name + ".jpg","idigbio-images-" + env + "-thumbnail")

    # Thumbnail short circuit
    if tkey.exists():
        db._cur.execute("UPDATE objects SET derivatives=true WHERE etag=%s", (name,))
        return False

    mkey = s.get_key(name,"idigbio-images-" + env)    
    fkey = s.get_key(name + ".jpg","idigbio-images-" + env + "-fullsize")
    wkey = s.get_key(name + ".jpg","idigbio-images-" + env + "-webview")
    

    if mkey.exists() and not all([fkey.exists(), wkey.exists(), tkey.exists()]):
        baseimg = img_from_key(mkey)

        if not fkey.exists():
            if baseimg.format == "JPEG":
                mkey.copy("idigbio-images-" + env + "-fullsize",name + ".jpg",metadata={'Content-Type': 'image/jpeg'})
                if fkey.exists():
                    fkey.make_public()
            else:
                fullbuff = save_to_buffer(baseimg,format="JPEG",quality=95)
                fkey.set_metadata('Content-Type', 'image/jpeg')
                fkey.set_contents_from_file(fullbuff)
                fkey.make_public()

        if not wkey.exists():
            webbuff = save_to_buffer(generate_derivative(baseimg,WEBVIEW_WIDTH),format="JPEG",quality=95)
            wkey.set_metadata('Content-Type', 'image/jpeg')
            wkey.set_contents_from_file(webbuff)
            wkey.make_public()

        if not tkey.exists():
            thumbbuff = save_to_buffer(generate_derivative(baseimg,THUMBNAIL_WIDTH),format="JPEG",quality=95)
            tkey.set_metadata('Content-Type', 'image/jpeg')
            tkey.set_contents_from_file(thumbbuff)
            tkey.make_public()

        db._cur.execute("UPDATE objects SET derivatives=true WHERE etag=%s", (name,))
        return True
    else:
        db._cur.execute("UPDATE objects SET derivatives=true WHERE etag=%s", (name,))
        return False

def main():
    db._cur.execute("SELECT * FROM objects where derivatives=false and bucket='images'")
    images = []
    for r in db._cur:
        images.append(r["etag"])

    c = Counter()

    print len(images)
    count = 0
    for etag in images:
        count += 1
        try:
            rv = check_and_generate(etag)
            c[rv] += 1
        except KeyboardInterrupt:
            raise KeyboardInterrupt
        except:
            print etag
            traceback.print_exc()

        if count % 100 == 0:
            print c.most_common()
            db.commit()

    print c.most_common()
    db.commit()

if __name__ == '__main__':
    main()