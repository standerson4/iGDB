import sys
import os
from pathlib import Path
import dbStructure
import Crawling_ASRank
import Crawling_EuroIX
import Crawling_HE
import Crawling_PCH
import Crawling_PDB
import Crawling_RIPEAtlas
import Crawling_RIPETraceroutes as Crawling_RIPETrace
import Crawling_Telegeography
import Processing_ASRank
import Processing_EuroIX
import Processing_PCH
import Processing_PDB
import Processing_RIPEAtlas
import Processing_RIPETraceroutes
import Processing_Submarine
import Processing_Voronoi
import Creating_Database
import Creating_OrgKML
import Querying_Database
import Plotting_ASNLocs
import Plotting_ShortestPath

class iGDB:
    def __init__(self, cli_args):
        self.print_help = False
        self.create_db = False
        self.create_db_name = ""
        self.process_data = False
        self.update_db = False
        self.update_location = ""
        self.query_db = False
        self.query_string = ""
        self.graph_asn = False
        self.graph_asn_num = ""
        self.hull_choice = False
        self.buffer_choice = False
        self.graph_shortest_path = False
        self.create_kml = False
        self.organization = ""
        self.start_loc = ""
        self.end_loc = ""
        self.valid_remote_locations = ["asrank", "euroix", "pch", "pdb", "he",
                "ripeatlas", "ripetraceroute", "telegeography"]
        self.unprocessed_path = Path("../unprocessed")
        self.processed_path = Path("../processed")
        self.database_path = Path("../database")
        self.plot_path = Path("../plots")
        self.helper_path = Path("../helper_data")
        for a in cli_args:
            if a == "-h" or "--help" in a:
                self.print_help = True
                break
            if a == "-c" or "--create_db" in a:
                self.create_db = True
            elif a == "-p" or "--process" in a:
                self.process_data = True
            elif a == "-u" or "--update" in a:
                self.update_db = True
            elif a == "-q" or "--query" in a:
                self.query_db = True
            elif a == "-ga" or a == "--graph-asn":
                self.graph_asn = True
            elif a == "-gab" or "--graph-asn-buffer" in a:
                self.graph_asn = True
                self.buffer_choice = True
            elif a == "-gac" or "--graph-asn-convex-hull" in a:
                self.graph_asn = True
                self.hull_choice = True
            elif a == "-gs" or "--graph-shortest-path" in a:
                self.graph_shortest_path = True
            elif a == "-k" or "--create_kml" in a:
                self.create_kml = True
            elif self.update_db and self.update_location == "":
                if a.lower() in self.valid_remote_locations:
                    self.update_location = a.lower()
                else:
                    self.update_db = False
                    print(f"{a} is an invalid update location.")
                    print(f"Please specify a valid update location from one of: ", end='')
                    print("{self.valid_remote_locations}.")
                    return
            elif self.create_db and self.create_db_name == "":
                self.create_db_name = a
            elif self.query_db:
                self.query_string += a + " "
            elif self.graph_asn:
                self.graph_asn_num = a
            elif self.graph_shortest_path and self.start_loc == "":
                self.start_loc = a
            elif self.graph_shortest_path and self.end_loc == "":
                self.end_loc = a
            elif self.create_kml and self.organization == "":
                self.organization = a

        if self.update_db and self.update_location == "":
            self.update_db = False
            print(f"Please specify a valid update location from one of: ", end='')
            print("{valid_remote_locations}.")

        if self.create_db and self.create_db_name == "":
            self.create_db = False
            print(f"Please specify a database name.")

        if self.graph_asn and self.graph_asn_num == "":
            self.graph_asn = False
            print(f"Please specify an ASN to graph.")

        if self.graph_shortest_path and (self.start_loc == "" or self.end_loc == ""):
            self.graph_shortest_path = False
            print(f"Please specify starting and ending locations to graph.")

        if self.create_kml and self.organization == "":
            self.create_kml = False
            print(f"Please specify an organization.")

    def run_steps(self):
        if self.print_help:
            self.print_help_func()
        elif self.create_db:
            self.create_db_func()
        elif self.process_data:
            self.process_local_data_func()
        elif self.update_db:
            print(f"Retrieving updated unprocessed data from {self.update_location}")
            self.update_db_func()
        elif self.query_db:
            self.query_db_func()
        elif self.graph_asn:
            self.plot_asn_locations()
        elif self.graph_shortest_path:
            self.plot_shortest_physical_path()
        elif self.create_kml:
            self.create_org_kml()
        else:
            self.print_help_func()

    def print_help_func(self):
        print("\nNAME")
        print("\tiGDB.py")
        print("DESCRIPTION")
        print("\tThis is the Internet Geographic Database (iGDB) utility. ", end='')
        print("It is used to execute these tasks:")
        print("\t\t* Create a database of logical and physical Internet infrastructure.")
        print("\t\t* Query the database for a better understanding of Internet infrastructure.")
        print("\t\t* Create visualizations that seek to elucidate the relationships")
        print("\t\t\tbetween the logical and physical nature of the Internet.")
        print("\tRun each task separately.")
        print("\nSYNOPSIS")
        print("\tpython iGDB.py [OPTIONS]")
        print("\nOPTIONS")
        print("\t-h or --help")
        print("\t\tprints this help menu")
        print("\t-c or --create_db <name> ")
        print("\t\tcreates a new database from local files.")
        print("\t\t<name> is the filename, created in the default location.")
        print("\t\tNOTE: Unformatted data must be processed with '-p' before this can be run.")
        print("\t-ga or --graph-asn <ASN> ")
        print("\t\tplot the nodes of <ASN> on a map.")
        print("\t-gab or --graph-asn-buffer <ASN> ")
        print("\t\tplot the nodes of <ASN> with a buffer around each node on a map.")
        print("\t-gac or --graph-asn-convex-hull <ASN> ")
        print("\t\tplot the nodes of <ASN> with a convex hull around all nodes on a map.")
        print('\t-gs or --graph-shortest-path "<START_CITY,START_STATE,START_COUNTRY>" ',end='')
        print('"<END_CITY,END_STATE,END_COUNTRY>"')
        print("\t\tplot the shortest inferred physical fiber between the specified cities on a map.")
        print("\t-k or --create_kml <ORGANIZATION>")
        print("\t\tcreate a KML file with the <ORGANIZATION> nodes and edges.")
        print("\t-p or --process")
        print("\t\tconverts unformatted local data files ", end='')
        print("into a format that can be added to the database")
        print("\t-q or --query <sql>")
        print("\t\texecutes a query of the iGIS database.")
        print("\t\t<sql> should be a valid SQL query")
        print("\t-u or --update <location>")
        print("\t\tqueries remote <location> ", end='')
        print("for updates to the local unprocessed information.")
        loc_string = ""
        for loc in self.valid_remote_locations:
            loc_string += f"'{loc}', "
        loc_string = loc_string[:-2]
        print(f"\t\t<location> must be one of: {loc_string}")
        print("\nREQUIREMENTS")
        print("\tThe utility is written for python 3.8 and requires these packages ", end='')
        print("(listed in requirements.txt):")
        print("\t\t* geopandas")
        print("\t\t* graphqlclient")
        print("\t\t* matplotlib")
        print("\t\t* networkx")
        print("\t\t* numpy")
        print("\t\t* pandas")
        print("\t\t* requests")
        print("\t\t* ripe.atlas.cousteau")
        print("\t\t* rtree")
        print("\t\t* selenium")
        print("\t\t* shapely")

    def create_db_func(self):
        db_creator = Creating_Database.CreatingDatabase(self.processed_path,
                self.database_path, self.create_db_name)

    def update_db_func(self):
        if not os.path.isdir(self.unprocessed_path):
            os.makedirs(self.unprocessed_path)

        if self.update_location.lower() == 'asrank':
            asrank_crawler = Crawling_ASRank.CrawlingASRank(self.unprocessed_path / 'ASRank')
            asrank_crawler.run_steps()
        elif self.update_location.lower() == 'euroix':
            euroix_crawler = Crawling_EuroIX.CrawlingEuroIX(self.unprocessed_path / 'EuroIX')
            euroix_crawler.run_steps()
        elif self.update_location.lower() == 'pdb':
            pdb_crawler = Crawling_PDB.CrawlingPDB(self.unprocessed_path / 'PeeringDB')
            pdb_crawler.run_steps()
        elif self.update_location.lower() == 'pch':
            pch_crawler = Crawling_PCH.CrawlingPCH(self.unprocessed_path / 'PCH')
            pch_crawler.run_steps()
        elif self.update_location.lower() == 'he':
            he_crawler = Crawling_HE.CrawlingHE(self.unprocessed_path / 'HE')
            he_crawler.run_steps()
        elif self.update_location.lower() == 'ripeatlas':
            ripe_crawler = Crawling_RIPEAtlas.CrawlingRIPEAtlas(self.unprocessed_path / 'RIPEAtlas', 'N')
            ripe_crawler.run_steps()
        elif self.update_location.lower() == 'ripetraceroute':
            trace_crawler = Crawling_RIPETrace.CrawlingRIPETraceroutes(self.unprocessed_path / 'RIPETraceroutes',
                    self.unprocessed_path / 'RIPEAtlas')
            trace_crawler.run_steps()
        elif self.update_location.lower() == 'telegeography':
            tele_crawler = Crawling_Telegeography.CrawlingTelegeography(self.unprocessed_path / 'Telegeography')
            tele_crawler.run_steps()

    def process_local_data_func(self):
        if not os.path.isdir(self.processed_path):
            os.makedirs(self.processed_path)

        # CAIDA ASRank processing
        asrank_processor = Processing_ASRank.ProcessingASRank(self.unprocessed_path / "ASRank",
                self.processed_path)
        asrank_processor.run_steps()

        # EuroIX processing
        print()
        euroix_processor = Processing_EuroIX.ProcessingEuroIX(self.unprocessed_path / "EuroIX",
                self.processed_path)
        euroix_processor.run_steps()

        # Packet Clearinghouse processing
        print()
        pch_processor = Processing_PCH.ProcessingPCH(self.unprocessed_path / 'PCH',
                self.processed_path)
        pch_processor.run_steps()

        # PeeringDB processing
        print()
        pdb_processor = Processing_PDB.ProcessingPDB(self.unprocessed_path / 'PeeringDB',
                self.processed_path)
        pdb_processor.run_steps()

        # RIPE Atlas anchors and probes processing
        print()
        ripe_processor = Processing_RIPEAtlas.ProcessingRIPEAtlas(self.unprocessed_path / 'RIPEAtlas',
                self.processed_path)
        ripe_processor.run_steps()

        # RIPE Atlas traceroute processing
        print()
        trace_processor = Processing_RIPETraceroutes.ProcessingRIPETraceroutes(self.unprocessed_path / 'RIPETraceroutes',
                self.processed_path)
        trace_processor.run_steps()

        # Telegeography submarine cables processing
        print()
        telegeography_processor = Processing_Submarine.ProcessingSubmarine(self.unprocessed_path / 'Telegeography',
                self.processed_path, self.helper_path / 'cities_Voronoi')
        telegeography_processor.run_steps()

        # Process the Voronoi diagram for the cities relations
        print()
        voronoi_processor = Processing_Voronoi.ProcessingVoronoi(self.helper_path / 'cities_Voronoi',
                self.processed_path)
        voronoi_processor.run_steps()

    def query_db_func(self):
        # we assume the first file in the database directory
        # is the database we care about
        try:
            f_name = os.listdir(self.database_path)[0]
        except:
            print("Database does not exist. Create database before querying. ", end='')
            print("Consult help menu (-h) for detailed instructions.")
            return ''

        #print(f"Querying {f_name} DB for '{self.query_string}'.")
        my_querier = Querying_Database.queryDatabase(self.database_path / f_name)
        my_results = my_querier.execute_query(self.query_string)
        # uncomment to print the results of the query
        print(f"{my_results}")
        return my_results

    def plot_asn_locations(self):
        self.query_string = "SELECT latitude, longitude FROM asn_loc "
        self.query_string += f"WHERE asn={self.graph_asn_num}; "
        asn_coords = self.query_db_func()
        if asn_coords:
            asn_plotter = Plotting_ASNLocs.PlottingASNLocs(self.graph_asn_num,
                    self.hull_choice, self.buffer_choice, asn_coords, self.plot_path)
            asn_plotter.plot()
        else:
            print(f"Sorry, there are no known locations for AS{self.graph_asn_num}.")

    def plot_shortest_physical_path(self):
        db_file = None
        for f in os.listdir(self.database_path):
            db_file = self.database_path / f

        my_plotter = Plotting_ShortestPath.PlottingShortestPath(db_file, self.start_loc, self.end_loc, self.plot_path)
        my_plotter.plot()

    def create_org_kml(self):
        db_file = None
        for f in os.listdir(self.database_path):
            db_file = self.database_path / f

        my_creator = Creating_OrgKML.CreatingOrgKML(db_file, self.organization, self.plot_path)
        my_creator.create_kml()

if __name__ == "__main__":
    my_igdb = iGDB(sys.argv)
    my_igdb.run_steps()
