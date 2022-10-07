import os
from pathlib import Path
import Querying_Database as qdb

class CreatingOrgKML:
    def __init__(self, db_file, org, out_dir):
        self.querier = qdb.queryDatabase(db_file)
        self.org = org
        self.out_dir = out_dir
        if not os.path.isdir(self.out_dir):
            os.makedirs(self.out_dir)

        self.nodes_list = []
        self.edges_list = []

    def create_kml(self):
        print(f"Executing queries to find nodes and edges for {self.org}. ", end='')
        print("(If the ISP is large, this could take a minute or two.)")
        self.execute_queries()
        if not self.nodes_list:
            print("No nodes found.")
        if not self.edges_list:
            print("No edges found.")
        if self.nodes_list or self.edges_list:
            print("Creating KML file.")
            save_file = self.out_dir / f"{self.org}_nodes_edges.kml"
            self.save_kml(save_file)

    def execute_queries(self):
        # first find all the nodes belonging to the organization
        n_query = f"""SELECT DISTINCT n.node_name, n.latitude, n.longitude
        FROM phys_nodes n
        WHERE n.organization LIKE "%{self.org}%"
        ORDER BY n.node_name
        ;"""

        self.nodes_list = self.querier.execute_query(n_query)

        # next find all the edges between the nodes
        e_query = f"""SELECT DISTINCT c.from_node, c.to_node, p.path_wkt
        FROM phys_nodes fn, phys_nodes tn, phys_nodes_conn c, standard_paths p
        WHERE fn.organization LIKE "%{self.org}%"
        AND tn.organization LIKE "%{self.org}%"
        AND c.from_node == fn.node_name
        AND c.to_node == tn.node_name

        AND ((p.from_city == fn.city
            AND p.from_country == fn.country
            AND p.to_city == tn.city
            AND p.to_country == tn.country) OR

            (p.to_city == fn.city
                AND p.to_country == fn.country
                AND p.from_city == tn.city
                AND p.from_country == tn.country)

            )

        ORDER BY fn.city
        ;"""

        edges = self.querier.execute_query(e_query)
        # sometimes there are duplicate edges, so we remove them
        for e in edges:
            fn = e[0]
            tn = e[1]
            wkt = e[2]
            wkt = wkt.replace("LINESTRING(", "")
            wkt = wkt.replace(")", "")
            unf = wkt.split(', ')
            coords = []
            for c in unf:
                coords.append(c.replace(' ', ','))
            if not (fn, tn, coords) in self.edges_list and not (tn, fn, coords) in self.edges_list:
                self.edges_list.append([fn, tn, coords])

    def save_kml(self, f_name):
        print(f"\tSaving nodes and edges to {f_name}.")
        header = """<?xml version="1.0" encoding="UTF-8"?>
            <kml xmlns="http://www.opengis.net/kml/2.2">
            <Document>\n"""
        header = header.replace('  ', '')
        footer = """</Document>
            </kml>\n"""
        footer = footer.replace('  ', '')
        with open(f_name, 'w') as f:
            f.write(header)
            for n in self.nodes_list:
                node = n[0]
                # this is for invalid tokens
                if '&' in node:
                    node = node.replace('&', ' and ')
                f.write("<Placemark>\n")
                f.write(f"<name>{node}</name>\n")
                f.write("<Point>\n")
                lat = n[1]
                lon = n[2]
                f.write("<coordinates>\n")
                f.write(f"{lon},{lat}\n")
                f.write("</coordinates>\n")
                f.write("</Point>\n")
                f.write("</Placemark>\n")

            for e in self.edges_list:
                f.write("<Placemark>\n")
                fn = e[0]
                tn = e[1]
                # this is for invalid tokens
                if '&' in fn:
                    fn = fn.replace('&', ' and ')
                if '&' in tn:
                    tn = tn.replace('&', ' and ')
                f.write(f"<name>{fn} to {tn}</name>\n")
                f.write("""<Style><LineStyle><color>ff0000ff</color>
                        <width>4</width></LineStyle>
                        <PolyStyle><fill>0</fill></PolyStyle></Style>\n""")
                f.write("<LineString>\n")
                f.write("<coordinates>\n")
                for c in e[2]:
                    f.write(f"{c}\n")

                f.write("</coordinates>\n")
                f.write("</LineString>\n")
                f.write("</Placemark>\n")

            f.write(footer)


if __name__ == "__main__":
    print("This script should not be run by itself. Run it through iGDB.py")
    # this is bogus testing data
    db_file = "../database/db_test.db"
    org = "AAPT"
    out_dir = Path("../plots")

    my_creator = CreatingOrgKML(db_file, org, out_dir)
    my_creator.create_kml()
