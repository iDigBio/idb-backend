from __future__ import absolute_import

import re
import os
import fiona
from shapely.geometry import shape, Point
from shapely.prepared import prep

from .memoize import memoized


pattern = re.compile(r'[\W_]+')

class ReverseGeocoder(object):

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
                        maxy = int(geo_shp.bounds[3])
                        for x in range(180+minx,180+maxx+1):
                            self.lon_box[x].add(k)

                        for y in range(90+miny,90+maxy+1):
                            self.lat_box[y].add(k)

                        self.countries[k] = prep(geo_shp)

    def get_country(self, lon, lat):
        if not (-90 <= lat <= 90) or not (-180 <= lon <= 180):
            return None
        p = Point(lon, lat)
        contries_consider = self.lat_box[90+int(lat)] & self.lon_box[180+int(lon)]
        for c in contries_consider:
            if self.countries[c].contains(p):
                return c
        return None


@memoized()
def get_rg():
    return ReverseGeocoder()


@memoized()
def get_rg_eez():
    return ReverseGeocoder(shapefile="data/EEZ_land_v2_201410.shp", cc_key="ISO_3digit")


def get_country(lon, lat, **kwargs):
    """Use a ReverseGeocoder to lookup the country code.

    Args like ReverseGeocoder.get_country()

    Additional Keyword args:

     * eez: boolean (Default: False). If set use the EEZ shapefiles

    """
    eez = kwargs.pop('eez', False)
    assert len(kwargs) == 0, "Unknown kwargs to get_country: {0!r}".format(kwargs)
    if eez:
        return get_rg_eez().get_country(lon, lat)
    else:
        return get_rg().get_country(lon, lat)
