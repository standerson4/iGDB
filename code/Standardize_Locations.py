from pathlib import Path
import geopandas as gpd
import numpy as np
from shapely.geometry import Point

class LocationStandardizer:
    def __init__(self, voronoi_dir):
        self.voronoi_dir = voronoi_dir
        self.voronoi_shapefile = self.voronoi_dir / "cities_Voronoi.shp"
        self._read_cities_shapefile()

    def _read_cities_shapefile(self):
        self.cities_df = gpd.read_file(self.voronoi_shapefile)
        self.cities_series = gpd.GeoSeries(self.cities_df.geometry)

    def standardize(self, node_coords):
        my_point = Point(node_coords[1], node_coords[0])
        contains = self.cities_series.contains(my_point)
        result = np.where(contains.values == True)
        if len(result) > 1:
            print(f"Error with: {node_coords}")
        try:
            std_lat = self.cities_df.iloc[result[0]]["LATITUDE"].values[0]
            std_lon = self.cities_df.iloc[result[0]]["LONGITUDE"].values[0]
            city = self.cities_df.iloc[result[0]]["NAME"].values[0]
            state = self.cities_df.iloc[result[0]]["ADM1NAME"].values[0]
            cc = self.cities_df.iloc[result[0]]["ISO_A2"].values[0]
            result_dict = {"LATITUDE":std_lat, "LONGITUDE":std_lon,
                    "CITY":city, "STATE":state, "COUNTRY":cc}
        except:
            print(f"\tNo lat/long for {node_coords}")
            result_dict = {}
        return result_dict

if __name__ == "__main__":
    voronoi_dir = Path("../helper_data/cities_Voronoi")
    my_standardizer = LocationStandardizer(voronoi_dir)
    chicago = [41.848, -87.699]
    print(f"Classifying a point ({chicago}) in Chicago.")
    result = my_standardizer.standardize(chicago)
    print(result)

    madrid = [40.326, -3.526]
    print(f"Classifying a point ({madrid}) in Madrid.")
    result = my_standardizer.standardize(madrid)
    print(result)
