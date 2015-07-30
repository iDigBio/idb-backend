import os
import fiona
from shapely.geometry import shape, Point
from shapely.prepared import prep

import re

pattern = re.compile('[\W_]+')

class ReverseGeocoder:

    def __init__(self, shapefile="data/world_borders.shp", cc_key="ISO3"):
        path = shapefile
        if not path.startswith("/"):
            path = os.path.join(os.path.dirname(__file__),shapefile)
        self.countries = {}
        with fiona.open(path) as shp:
            for g in shp:
                if cc_key in g["properties"]:
                    k = pattern.sub('', g["properties"][cc_key]).lower()
                    if len(k) == 3:
                        self.countries[k] = prep(shape(g["geometry"]))

    def get_country(self, lon, lat):
        p = Point(lon,lat)
        for c in self.countries:
            if self.countries[c].contains(p):
                return c
        return None
