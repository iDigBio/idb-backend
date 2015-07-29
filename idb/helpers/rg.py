import fiona
from shapely.geometry import shape, Point
from shapely.prepared import prep

class ReverseGeocoder:

    def __init__(self, shapefile="data/world_borders.shp"):
        self.countries = {}
        with fiona.open(shapefile) as shp:
            for g in shp:
                self.countries[g["properties"]["ISO3"]] = prep(shape(g["geometry"]))

    def get_country(self, lon, lat):
        p = Point(lon,lat)
        for c in self.countries:
            if self.countries[c].contains(p):
                return c
        return None
