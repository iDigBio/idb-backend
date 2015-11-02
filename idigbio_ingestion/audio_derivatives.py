import os
import sys
import traceback

import datetime

from idb.helpers.storage import IDigBioStorage

import cStringIO

from collections import Counter

from idb.postgres_backend.db import PostgresDB

THUMBNAIL_WIDTH = 260
WEBVIEW_WIDTH = 600

s = IDigBioStorage()
db = PostgresDB()

from pydub import AudioSegment
from PIL import Image, ImageDraw, ImageFont

import fontconfig
fonts = fontconfig.query(family='ubuntu', lang='en')
font_file = None
for f in fonts:
    if f.endswith("Ubuntu-B.ttf"):
        font_file = f
        break
else:
    font_file = "/usr/share/fonts/truetype/ubuntu-font-family/Ubuntu-R.ttf"


# From https://gist.github.com/mixxorz/abb8a2f22adbdb6d387f
class Waveform(object):

    bar_count = 107
    db_ceiling = 60

    def __init__(self, filename, name=None):
        self.file = filename
        if name is not None:
            self.name = name
        else:
            self.name = self.file.split('.')[0]

        self._img = None

        audio_file = AudioSegment.from_file(
            self.file, "mp3")

        # Length in miliseconds
        self.length = len(audio_file)

        self.peaks = self._calculate_peaks(audio_file)

    def _calculate_peaks(self, audio_file):
        """ Returns a list of audio level peaks """
        chunk_length = len(audio_file) / self.bar_count

        loudness_of_chunks = [
            audio_file[i * chunk_length: (i + 1) * chunk_length].rms
            for i in range(self.bar_count)]

        max_rms = max(loudness_of_chunks) * 1.00

        return [int((loudness / max_rms) * self.db_ceiling)
                for loudness in loudness_of_chunks]

    def _get_bar_image(self, size, fill):
        """ Returns an image of a bar. """
        width, height = size
        bar = Image.new('RGBA', size, fill)

        end = Image.new('RGBA', (width, 2), fill)
        draw = ImageDraw.Draw(end)
        draw.point([(0, 0), (3, 0)], fill='#c1c1c1')
        draw.point([(0, 1), (3, 1), (1, 0), (2, 0)], fill='#555555')

        bar.paste(end, (0, 0))
        bar.paste(end.rotate(180), (0, height - 2))
        return bar

    def _generate_waveform_image(self):
        """ Returns the full waveform image """
        if self._img is None:
            im = Image.new('RGB', (840, 150), '#f5f5f5')
            for index, value in enumerate(self.peaks, start=0):
                column = index * 8 + 2
                upper_endpoint = 64 - value

                im.paste(self._get_bar_image((4, value * 2), '#424242'),
                         (column, upper_endpoint))

            draw = ImageDraw.Draw(im)
            font = ImageFont.truetype(font_file, 16)

            t = (datetime.datetime.min + datetime.timedelta(milliseconds=self.length)).time()
            draw.text((0, 128),"Duration: " + t.isoformat(),'#424242',font=font)

            self._img = im

        return self._img

    def save(self, toFile=True):
        """ Save the waveform as an image """
        if toFile:
            png_filename = self.name + '.png'
            with open(png_filename, 'wb') as imfile:
                self._generate_waveform_image().save(imfile, format="JPEG",quality=95)
        else:
            dervbuff = cStringIO.StringIO()
            self._generate_waveform_image().save(dervbuff, format="JPEG",quality=95)
            dervbuff.seek(0)
            return dervbuff

def save_to_buffer(img, **kwargs):
    dervbuff = cStringIO.StringIO()
    img.save(dervbuff,**kwargs)
    dervbuff.seek(0)
    return dervbuff

def buff_from_key(key):
    try:
        buff = cStringIO.StringIO()
        key.get_contents_to_file(buff)
        buff.seek(0)
        return buff
    except Exception as e:
        raise Exception(traceback.format_exc() +"Exception during processing of "+key.name)

def generate_derivative(img, derivative_width):
    if img.size[0] > derivative_width:
        derivative_width_percent = (derivative_width / float(img.size[0]))
        derivative_horizontal_size = int((float(img.size[1]) * float(derivative_width_percent)))
        derv = img.resize((derivative_width, derivative_horizontal_size), Image.BILINEAR)
        return derv
    else:
        return img        

def check_and_generate(name,env="prod"):
    tkey = s.get_key(name + ".jpg","idigbio-sounds-" + env + "-thumbnail")

    # Thumbnail short circuit
    if tkey.exists():
        db._cur.execute("UPDATE objects SET derivatives=true WHERE etag=%s", (name,))
        return False

    mkey = s.get_key(name,"idigbio-sounds-" + env)    
    fkey = s.get_key(name + ".jpg","idigbio-sounds-" + env + "-fullsize")
    wkey = s.get_key(name + ".jpg","idigbio-sounds-" + env + "-webview")
    

    if mkey.exists() and not all([fkey.exists(), wkey.exists(), tkey.exists()]):
        basefile = buff_from_key(mkey)
        w = Waveform(basefile,name=name)
        d = w.save(toFile=False)    

        if not fkey.exists():
            fkey.set_metadata('Content-Type', 'image/jpeg')
            fkey.set_contents_from_file(d)
            fkey.make_public()

        if not wkey.exists():
            webbuff = save_to_buffer(generate_derivative(w._img,WEBVIEW_WIDTH),format="JPEG",quality=95)
            wkey.set_metadata('Content-Type', 'image/jpeg')
            wkey.set_contents_from_file(webbuff)
            wkey.make_public()

        if not tkey.exists():
            thumbbuff = save_to_buffer(generate_derivative(w._img,THUMBNAIL_WIDTH),format="JPEG",quality=95)
            tkey.set_metadata('Content-Type', 'image/jpeg')
            tkey.set_contents_from_file(thumbbuff)
            tkey.make_public()

        db._cur.execute("UPDATE objects SET derivatives=true WHERE etag=%s", (name,))
        return True
    else:
        db._cur.execute("UPDATE objects SET derivatives=true WHERE etag=%s", (name,))
        return False            

def main():
    db._cur.execute("SELECT * FROM objects where derivatives=false and bucket='sounds'")
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