from shortest_physical_path import parse_arguments
import sqlite3
import networkx as nx
from networkx.exception import NetworkXNoPath
from collections import defaultdict
import Querying_Database as qdb
from shapely.geometry import Point
from shapely import wkt
import ast
import sys
import math

global_graph = nx.Graph()
global_edges_dict = {}
global_nodes_dict = {}
global_querier = None


def get_distance_between_two_router(from_place, to_place):
    src = from_place
    dst = to_place
    route = None
    dist = 0.0
    route_geom = []
    waypoints_geom = []

    cities_valid = are_cities_valid(src, dst)
    if cities_valid:
        route = get_shortest_path(src, dst, route_geom, waypoints_geom)
        if route:
            dist = calc_dist_along_path(route)
        else:
            dist = math.nan
    return dist


def is_city_valid(city):
    # Check the input structure and build the query
    parts = city.split(',')
    query = ""
    if len(parts) == 3:
        query = f"SELECT * FROM city_points WHERE city_name='{parts[0].strip()}' AND state_province='{parts[1].strip()}' AND country_code='{parts[2].strip()}';"
    elif len(parts) == 2:
        query = f"SELECT * FROM city_points WHERE city_name='{parts[0].strip()}' AND country_code='{parts[1].strip()}';"
    else:
        print("Please specify a city/country or city/state/country", file=sys.stderr)
        return False, ""

    # Verify the city is in the DB
    results = global_querier.execute_query(query)
    if len(results) == 0:
        print(f"{city} not found in the database.", file=sys.stderr)
        return False, ""
    if len(results) > 1:
        print("Encountered multiple entries for the city in the database.",
              file=sys.stderr)
        print(f"{city} matches multiple entries in the database. Please specify more details.", file=sys.stderr)
        print("Matching cities:", file=sys.stderr)
        for r in results:
            print(f"\t{r[0]}, {r[1]}, {r[2]}", file=sys.stderr)
        return False, ""
    if len(results) == 1:
        # Construct the full city representation from results if not specified by user
        city = f"{results[0][0]},{results[0][1]},{results[0][2]}"
        return True


def are_cities_valid(src, dst):
    if src == dst:
        print("Source and destination cannot be the same", file=sys.stderr)
        return False

    # Validate the source city
    if (is_city_valid(src) and is_city_valid(dst)):
        return True

    return False


def query_db_for_nodes():
    nodes_query = f"""SELECT * FROM city_points;"""
    results = global_querier.execute_query(nodes_query)
    for row in results:
        city = row[0]
        state = row[1]
        country = row[2]
        lat = float(row[3])
        lon = float(row[4])
        global_nodes_dict[(city, state, country)] = {}
        global_nodes_dict[(city, state, country)]['x'] = lon
        global_nodes_dict[(city, state, country)]['y'] = lat
        global_nodes_dict[(city, state, country)]['GEOM'] = Point(lon, lat)


def query_db_for_edges():
    nodes_query = f"""SELECT * FROM standard_paths;"""
    results = global_querier.execute_query(nodes_query)
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
        global_edges_dict[edge] = {}
        global_edges_dict[edge]['DIST_KM'] = dist_km
        global_edges_dict[edge]['GEOM'] = wkt.loads(path_wkt)


def create_graph():
    # add nodes
    for n in global_nodes_dict.keys():
        x = global_nodes_dict[n]['x']
        y = global_nodes_dict[n]['y']
        geom = global_nodes_dict[n]['GEOM']
        global_graph.add_node(n, x=x, y=y, geom=geom)
    # add edges
    for e in global_edges_dict.keys():
        dist = global_edges_dict[e]['DIST_KM']
        geom = global_edges_dict[e]['GEOM']
        global_graph.add_edge(e[0], e[1], dist_km=dist, geom=geom)


def initialize_the_global_graph(db_file):
    global global_querier
    global_querier = qdb.queryDatabase(db_file)
    query_db_for_nodes()
    query_db_for_edges()
    create_graph()


def get_shortest_path(src, dst, route_geom, waypoints_geom):
    fc = src.split(',')[0].strip()
    fs = src.split(',')[1].strip()
    fcc = src.split(',')[2].strip()
    tc = dst.split(',')[0].strip()
    ts = dst.split(',')[1].strip()
    tcc = dst.split(',')[2].strip()

    src = (fc, fs, fcc)
    dst = (tc, ts, tcc)

    try:
        route = nx.shortest_path(global_graph, src, dst, weight='dist_km')
        for i in range(len(route)-1):
            e = (route[i], route[i+1])
            if not e in global_edges_dict.keys():
                e = (route[i+1], route[i])
            geom = global_edges_dict[e]["GEOM"]
            route_geom.append(geom)
            if not global_nodes_dict[e[0]]["GEOM"] in waypoints_geom:
                waypoints_geom.append(global_nodes_dict[e[0]]["GEOM"])
            if not global_nodes_dict[e[1]]["GEOM"] in waypoints_geom:
                waypoints_geom.append(global_nodes_dict[e[1]]["GEOM"])
        return route
    except:
        print(
            f"Could not complete query from '{src}' to '{dst}'.", file=sys.stderr)
        return None


def calc_dist_along_path(route):
    dist = 0.0
    for i in range(len(route)-1):
        e = (route[i], route[i+1])
        if not e in global_edges_dict.keys():
            e = (route[i+1], route[i])
        dist += global_edges_dict[e]["DIST_KM"]
    return dist


def transform_location_format(location):
    return location.replace('/', ',')


def update_database(db_file):
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    cursor.execute('UPDATE phys_nodes AS pnc \
    SET state = cp.state_province \
    FROM city_points AS cp \
    WHERE ABS(pnc.latitude - cp.city_latitude) < 0.001\
    AND ABS(pnc.longitude - cp.city_longitude) < 0.001 \
    AND pnc.city = cp.city_name \
    AND pnc.country = cp.country_code;')

    conn.commit()
    conn.close()


def create_graph_from_phys_nodes(nodes, connections, start_city, start_state, start_country, end_city, end_state, end_country):
    G = nx.DiGraph()

    # Add nodes to the graph
    for node in nodes:
        G.add_node(node[0], city=node[1], state=node[2], country=node[3])

    # Add edges (connections) to the graph
    for connection in connections:
        if not connection[0] not in G.nodes and connection[1] not in G.nodes:
            continue
        G.add_edge(connection[0], connection[1])
        # assume symmetric connections
        G.add_edge(connection[1], connection[0])

    # Find nodes matching the start and end city/state pairs
    start_nodes = [node[0] for node in nodes if node[1] == start_city and
                   (node[2] == start_state or not start_state) and
                   node[3] == start_country]
    end_nodes = [node[0] for node in nodes if node[1] == end_city and
                 (node[2] == end_state or not end_state) and
                 node[3] == end_country]

    print('# of start nodes:', len(start_nodes), file=sys.stderr)
    print('# of end nodes:', len(end_nodes), file=sys.stderr)

    return G, start_nodes, end_nodes


def shortest_path_distribution(G, start_nodes, end_nodes):
    def _node_str(n):
        return f'{n["city"]}/{n["state"]}/{n["country"]}'

    # Find the shortest path for each combination of start and end nodes
    all_paths = []
    for start_node in start_nodes:
        for end_node in end_nodes:
            try:
                shortest_path = nx.shortest_path(
                    G, source=start_node, target=end_node)
                shortest_path_location = [
                    _node_str(G.nodes[node]) for node in shortest_path]
                all_paths.append(shortest_path_location)
            except NetworkXNoPath:
                pass

    path_distribution = defaultdict(int)
    for path in all_paths:
        path_distribution[str(path)] += 1

    return path_distribution


def print_shortest_paths_and_distances(path_distribution):
    table_rows = []

    for path, count in path_distribution.items():
        path = transform_location_format(path)
        path = ast.literal_eval(path)

        print(f"{count}: {path}", file=sys.stderr)

        print("Calculating the whole distance for the upper path", file=sys.stderr)
        distance_sum = 0
        distance_list = []
        for i in range(1, len(path)):
            single_distance = get_distance_between_two_router(
                path[i-1], path[i])
            distance_list.append(single_distance)
            distance_sum += single_distance
        print("the distance for the whole path is", distance_sum,
              "it is consist of", distance_list, file=sys.stderr)
        # Join items within each list
        path_list = '|'.join(map(str, path))
        distance_list = '|'.join(map(str, distance_list))

        # Combine both strings into one table row
        table_row = str(count) + '\t' + str(distance_sum) + \
            '\t' + path_list + '\t' + distance_list
        if (not math.isnan(distance_sum)):
            table_rows.append(table_row)
    table = '\n'.join(table_rows)

    print(table)


def find_shortest_path(db_file, start_city, start_state, start_country, end_city, end_state, end_country):
    # Connect to the SQLite database
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Query the nodes and connections from the database
    cursor.execute('SELECT node_name, city, state, country FROM phys_nodes')
    nodes = cursor.fetchall()

    cursor.execute('SELECT from_node, to_node FROM phys_nodes_conn')
    connections = cursor.fetchall()

    G, start_nodes, end_nodes = create_graph_from_phys_nodes(
        nodes, connections, start_city, start_state, start_country, end_city, end_state, end_country)

    path_distribution = shortest_path_distribution(G, start_nodes, end_nodes)

    print(
        f"Path distribution from {start_city}, {start_state}, {start_country} to {end_city},{end_state}, {end_country}:", file=sys.stderr)

    print_shortest_paths_and_distances(path_distribution)

    conn.close()


if __name__ == "__main__":
    """Remember to run 'python3 iGDB.py -c database_name.db' for further user"""
    # Parse command-line arguments
    src, dst = parse_arguments()

    """the update_database function is requied to run for the first time you use the script, comment it after that."""
    update_database('../database/igdb.db')

    initialize_the_global_graph('../database/igdb.db')

    find_shortest_path('../database/igdb.db',
                       src[0], src[1], src[2],
                       dst[0], dst[1], dst[2])
