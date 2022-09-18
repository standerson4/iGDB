import os
from pathlib import Path
import geopandas as gpd
from shapely import wkt
from shapely.geometry import LineString
from shapely.geometry import Point
import networkx as nx
import matplotlib.pyplot as plt
import Querying_Database as qdb
import sys


class PlottingShortestPath:
    def __init__(self, db_file, from_place, to_place, out_dir):
        self.querier = qdb.queryDatabase(db_file)
        self.src = from_place
        self.dst = to_place
        self.out_dir = out_dir
        if not os.path.isdir(self.out_dir):
            os.makedirs(self.out_dir)

        self.world_countries = "../helper_data/World_Countries_(Generalized)/World_Countries__Generalized_.shp"
        self.nodes_dict = {}
        self.edges_dict = {}
        self.standard_graph = nx.Graph()
        self.route = None
        self.dist = 0.0
        self.route_geom = []
        self.waypoints_geom = []

    def plot(self):
        print(f"Finding and plotting shortest path from '{self.src}' to '{self.dst}'.")
        cities_valid = self.are_cities_valid()
        if cities_valid:
            self.query_db_for_nodes()
            self.query_db_for_edges()
            self.create_graph()
            self.get_shortest_path()
            self.dist = self.calc_dist_along_path(self.route)
            print(f"\nRoute: {self.route}")
            print(f"Distance along route: {self.dist:.2f} km.\n")
            self.make_plot()

    def are_cities_valid(self):
        if len(self.src.split(',')) == 3:
            fc = self.src.split(',')[0].strip()
            fs = self.src.split(',')[1].strip()
            fcc = self.src.split(',')[2].strip()
            f_query = f"""SELECT * 
                FROM city_points
                WHERE city_name='{fc}' AND state_province='{fs}' AND country_code='{fcc}';"""
        elif len(self.src.split(',')) == 2:
            fc = self.src.split(',')[0].strip()
            fcc = self.src.split(',')[1].strip()
            f_query = f"""SELECT * 
                FROM city_points
                WHERE city_name='{fc}' AND country_code='{fcc}';"""
        else:
            print("Please specify a city/country or city/state/country")
            return False

        if len(self.dst.split(',')) == 3:
            tc = self.dst.split(',')[0].strip()
            ts = self.dst.split(',')[1].strip()
            tcc = self.dst.split(',')[2].strip()
            t_query = f"""SELECT * 
                FROM city_points
                WHERE city_name='{tc}' AND state_province='{ts}' AND country_code='{tcc}';"""
        elif len(self.dst.split(',')) == 2:
            tc = self.dst.split(',')[0].strip()
            tcc = self.dst.split(',')[1].strip()
            t_query = f"""SELECT * 
                FROM city_points
                WHERE city_name='{tc}' AND country_code='{tcc}';"""
        else:
            print("Please specify a city/country or city/state/country")
            return False

        # verify the source node is in the DB
        results = self.querier.execute_query(f_query)
        if len(results) == 0:
            print(f"{self.src} not found in the database.")
            return False
        if len(results) > 1:
            print(f"{self.src} matches multiple entries in the database. Please specify a state/province.")
            print("Matching cities:")
            for r in results:
                print(f"\t{r[0]}, {r[1]}, {r[2]}")
            return False
        if len(results) == 1:
            fc = results[0][0]
            fs = results[0][1]
            fcc = results[0][2]

        # verify the destination node is in the DB
        results = self.querier.execute_query(t_query)
        if len(results) == 0:
            print(f"{self.dst} not found in the database.")
            return False
        if len(results) > 1:
            print(f"{self.dst} matches multiple entries in the database. Please specify a state/province.")
            print("Matching cities:")
            for r in results:
                print(f"\t{r[0]}, {r[1]}, {r[2]}")
            return False
        if len(results) == 1:
            tc = results[0][0]
            ts = results[0][1]
            tcc = results[0][2]

        # necessary if the user does not enter a state
        self.src = f"{fc},{fs},{fcc}"
        self.dst = f"{tc},{ts},{tcc}"

        return True

    def query_db_for_nodes(self):
        #print("Querying the DB for the graph nodes.")

        nodes_query = f"""SELECT * FROM city_points;"""
        results = self.querier.execute_query(nodes_query)
        for row in results:
            city = row[0]
            state = row[1]
            country = row[2]
            lat = float(row[3])
            lon = float(row[4])
            self.nodes_dict[(city, state, country)] = {}
            self.nodes_dict[(city, state, country)]['x'] = lon
            self.nodes_dict[(city, state, country)]['y'] = lat
            self.nodes_dict[(city, state, country)]['GEOM'] = Point(lon, lat)

    def query_db_for_edges(self):
        #print("Querying the DB for the graph edges.")

        nodes_query = f"""SELECT * FROM standard_paths;"""
        results = self.querier.execute_query(nodes_query)
        for row in results:
            fc = row[0]
            fs = row[1]
            fcc = row[2]
            tc = row[3]
            ts = row[4]
            tcc = row[5]
            dist_km = float(row[6])
            path_wkt = row[7]
            edge = ((fc, fs, fcc), (tc, ts, tcc))
            self.edges_dict[edge] = {}
            self.edges_dict[edge]['DIST_KM'] = dist_km
            self.edges_dict[edge]['GEOM'] = wkt.loads(path_wkt)

    def create_graph(self):
        #print("Creating the graph.")
        # add nodes
        for n in self.nodes_dict.keys():
            x = self.nodes_dict[n]['x']
            y = self.nodes_dict[n]['y']
            geom = self.nodes_dict[n]['GEOM']
            self.standard_graph.add_node(n, x=x, y=y, geom=geom)
        # add edges
        for e in self.edges_dict.keys():
            dist = self.edges_dict[e]['DIST_KM']
            geom = self.edges_dict[e]['GEOM']
            self.standard_graph.add_edge(e[0], e[1], dist_km=dist, geom=geom)

    def get_shortest_path(self):
        fc = self.src.split(',')[0].strip()
        fs = self.src.split(',')[1].strip()
        fcc = self.src.split(',')[2].strip()
        tc = self.dst.split(',')[0].strip()
        ts = self.dst.split(',')[1].strip()
        tcc = self.dst.split(',')[2].strip()

        src = (fc, fs, fcc)
        dst = (tc, ts, tcc)

        try:
            self.route = nx.shortest_path(self.standard_graph, src, dst, weight='dist_km')
            for i in range(len(self.route)-1):
                e = (self.route[i], self.route[i+1])
                if not e in self.edges_dict.keys():
                    e = (self.route[i+1], self.route[i])
                geom = self.edges_dict[e]["GEOM"]
                self.route_geom.append(geom)
                if not self.nodes_dict[e[0]]["GEOM"] in self.waypoints_geom:
                    self.waypoints_geom.append(self.nodes_dict[e[0]]["GEOM"])
                if not self.nodes_dict[e[1]]["GEOM"] in self.waypoints_geom:
                    self.waypoints_geom.append(self.nodes_dict[e[1]]["GEOM"])
        except:
            print(f"Could not complete query from '{self.src}' to '{self.dst}'.")

    def calc_dist_along_path(self, route):
        dist = 0.0
        for i in range(len(route)-1):
            e = (route[i], route[i+1])
            if not e in self.edges_dict.keys():
                e = (route[i+1], route[i])
            dist += self.edges_dict[e]["DIST_KM"]
        return dist

    def make_plot(self):
        #print("Making plot")
        colors = ['#7fc97f','#beaed4','#fdc086','#ffff99','#386cb0','#f0027f','#bf5b17']
        world = gpd.read_file(self.world_countries)
        base = world.plot(color='#c2c6cc', alpha=0.4, edgecolor='#cccccc', figsize=(30,20))

        lines_df = gpd.GeoDataFrame(geometry=self.route_geom)
        lines_df.plot(ax=base, color=colors[1], zorder=1, linewidth=5)
        points_df = gpd.GeoDataFrame(geometry=self.waypoints_geom)
        points_df.plot(ax=base, color=colors[4], zorder=2,
                marker='o', markersize=30, alpha=1)
        ln_minx, ln_miny, ln_maxx, ln_maxy = lines_df.total_bounds
        us_miny = 24.4
        us_minx = -126.9
        us_maxy = 51.0
        us_maxx = -63.3

        eu_miny = 35.4
        eu_minx = -13.6
        eu_maxy = 59.3
        eu_maxx = 42.3

        if ln_minx > us_minx and ln_maxx < us_maxx and ln_miny > us_miny and ln_maxy < us_maxy:
            miny = us_miny
            minx = us_minx
            maxy = us_maxy
            maxx = us_maxx
            base.set_xlim(minx, maxx)
            base.set_ylim(miny, maxy)
        elif ln_minx > eu_minx and ln_maxx < eu_maxx and ln_miny > eu_miny and ln_maxy < eu_maxy:
            miny = eu_miny
            minx = eu_minx
            maxy = eu_maxy
            maxx = eu_maxx
            base.set_xlim(minx, maxx)
            base.set_ylim(miny, maxy)

        fc = self.src.split(',')[0].strip()
        tc = self.dst.split(',')[0].strip()
        plt.axis('off')
        save_file = self.out_dir / f"{fc}_{tc}_shortest_path.png"
        print(f"Saving to {save_file}") 
        plt.savefig(save_file)


if __name__ == "__main__":
    print("This script should not be run by itself. Run it through iGDB.py")
    # this is bogus testing data
    db_file = "../database/db_test.db"
    #from_place = "Los Angeles,California,US"
    #to_place = "Minneapolis,Minnesota,US"
    #from_place = "Bend,Oregon,US"
    #to_place = "Miami,Florida,US"
    #to_place = "Orlando,Florida,US"
    #from_place = "Lisbon,PT"
    #to_place = "Warsaw,PL"
    from_place = "Madrid,Comunidad de Madrid,ES"
    to_place = "Naples,IT"
    to_place = "Sibiu,RO"
    #from_place = "Singapore,SG"
    #to_place = "Chennai,IN"
    out_dir = Path("../plots")

    my_plotter = PlottingShortestPath(db_file, from_place, to_place, out_dir)
    my_plotter.plot()
