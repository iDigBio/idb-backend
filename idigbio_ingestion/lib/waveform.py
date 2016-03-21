from __future__ import absolute_import
from __future__ import print_function
from __future__ import division

import datetime
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


# Based on https://gist.github.com/mixxorz/abb8a2f22adbdb6d387f
class Waveform(object):
    bar_count = 107
    db_ceiling = 60

    def __init__(self, file, name=None):
        audio_file = AudioSegment.from_file(file, "mp3")

        # Length in miliseconds
        self.length = len(audio_file)

        self.peaks = self._calculate_peaks(audio_file)

    def _calculate_peaks(self, audio_file):
        """ Returns a list of audio level peaks """
        chunk_length = len(audio_file) // self.bar_count

        loudness_of_chunks = [
            audio_file[i * chunk_length:(i + 1) * chunk_length].rms
            for i in range(self.bar_count)
        ]

        max_rms = max(loudness_of_chunks) * 1.00

        return [int((loudness // max_rms) * self.db_ceiling)
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

    def generate_waveform_image(self):
        """ Returns the full waveform image """

        im = Image.new('RGB', (840, 150), '#f5f5f5')
        for index, value in enumerate(self.peaks, start=0):
            column = index * 8 + 2
            upper_endpoint = 64 - value

            im.paste(
                self._get_bar_image(
                    (4, value * 2), '#424242'), (column, upper_endpoint))

        draw = ImageDraw.Draw(im)
        font = ImageFont.truetype(font_file, 16)
        t = (datetime.datetime.min + datetime.timedelta(milliseconds=self.length))
        text = "Duration: " + t.time().isoformat()
        draw.text((0, 128), text, '#424242', font=font)

        return im
