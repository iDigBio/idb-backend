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
        self.lat_box = [set() for i in range(0,181)]
        self.lon_box = [set() for i in range(0,361)]

        with fiona.open(path) as shp:
            for g in shp:
                if cc_key in g["properties"]:
                    k = pattern.sub('', g["properties"][cc_key]).lower()
                    if len(k) == 3:
                        geo_shp = shape(g["geometry"])
                        minx = int(geo_shp.bounds[0])
                        maxx = int(geo_shp.bounds[2])
                        miny = int(geo_shp.bounds[1])
                        maxy = int(geo_shp.bounds[1])
                        for x in range(180+minx,180+maxx+1):
                            self.lon_box[x].add(k)

                        for y in range(90+miny,90+maxy+1):
                            self.lat_box[y].add(k)

                        self.countries[k] = prep(geo_shp)

    def get_country(self, lon, lat):
        p = Point(lon,lat)
        contries_consider = self.lat_box[90+int(lat)] & self.lon_box[180+int(lon)]
        for c in contries_consider:
            if self.countries[c].contains(p):
                return c
        return None
