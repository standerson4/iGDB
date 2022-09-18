import os
from pathlib import Path
import geopandas as gpd
from shapely.geometry import MultiPoint
import matplotlib.pyplot as plt

class PlottingASNLocs:
    def __init__(self, asn, hull_choice, buffer_choice, in_points, out_dir):
        self.asn = asn
        self.points = in_points
        self.out_dir = out_dir
        self.draw_hull = hull_choice
        self.draw_buffer = buffer_choice

        if not os.path.isdir(self.out_dir):
            os.makedirs(self.out_dir)

    def plot(self):
        print(f"Plotting AS{self.asn} geolocation and saving to '{self.out_dir}/plot_{self.asn}'")
        self.make_points_gdf()
        self.make_plot()

    def make_points_gdf(self):
        x_vals = []
        y_vals = []
        xy_vals = []
        for p in self.points:
            x_vals.append(p[1])
            y_vals.append(p[0])
            xy_vals.append((x_vals[-1], y_vals[-1]))

        # these are the individual nodes where the ASN is located
        self.asn_points = gpd.GeoDataFrame(geometry=gpd.points_from_xy(x_vals, y_vals))

        # this is the convex hull surrounding the nodes
        #ch = MultiPoint(self.points).convex_hull
        ch = MultiPoint(xy_vals).convex_hull
        gs = gpd.GeoSeries.from_wkt([ch.wkt])
        self.convex_hull = gpd.GeoDataFrame(geometry=gs)

        # these are the buffers surrounding the nodes
        bu = MultiPoint(xy_vals).buffer(2)
        gs = gpd.GeoSeries.from_wkt([bu.wkt])
        self.pt_buffer = gpd.GeoDataFrame(geometry=gs)

    def make_plot(self):
        world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
        base = world.plot(color='#edf8fb', edgecolor='#cccccc', figsize=(15,10))

        if self.draw_hull:
            self.convex_hull.plot(ax=base, color='#fdcc8a', alpha=0.5, edgecolor='#fdcc8a')
        if self.draw_buffer:
            self.pt_buffer.plot(ax=base, color='#fdcc8a', alpha=0.5, edgecolor='#fdcc8a')

        self.asn_points.plot(ax=base, marker='o', color='#b30000', markersize=10)
        plt.title(f"AS{self.asn} nodes and coverage area")
        plt.axis('off')
        plt.savefig(self.out_dir / f"plot_{self.asn}.png")


if __name__ == "__main__":
    print("This script should not be run by itself. Run it through iGDB.py")
    # this is bogus testing data
    asn = -1
    my_points = [(42.35, -71.06), (38.19, -78.0), (41.5, -80.87), (39.9, -78.0)]
    out_dir = Path("../plots")

    my_plotter = PlottingASNLocs(asn, True, False, my_points, out_dir)
    my_plotter.plot()
