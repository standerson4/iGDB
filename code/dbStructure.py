"""These SQL statements are used to create each table in the DB.
The DB structure should all be defined in this file."""

sql_create_city_points_table = """ CREATE TABLE IF NOT EXISTS city_points(
                                        city_name text,
                                        state_province text,
                                        country_code text,
                                        city_latitude numeric,
                                        city_longitude numeric
                                    ); """

sql_create_city_polygons_table = """ CREATE TABLE IF NOT EXISTS city_polygons(
                                        city_name text,
                                        state_province text,
                                        country_code text,
                                        polygon_wkt text
                                    ); """

sql_create_ip_asn_dns_table = """ CREATE TABLE IF NOT EXISTS ip_asn_dns(
                                        ip_addr text,
                                        rdns text,
                                        asn integer,
                                        standard_city text,
                                        standard_state text,
                                        standard_country text,
                                        source text,
                                        asof_date date
                                    ); """

sql_create_ip_dns_table = """ CREATE TABLE IF NOT EXISTS ip_dns(
                                        ip_addr text,
                                        rdns text,
                                        asn integer,
                                        rdns_geography text
                                    ); """

sql_create_ip_inference_table = """ CREATE TABLE IF NOT EXISTS ip_inference(
                                        ip_addr text,
                                        geography_inference text
                                    ); """

sql_create_traceroutes_table = """ CREATE TABLE IF NOT EXISTS traceroutes(
                                        source_ip text,
                                        destination_ip text,
                                        hop_ip text,
                                        TTL integer,
                                        RTT numeric,
                                        source text,
                                        timestamp integer,
                                        asof_date date
                                    ); """

sql_create_asn_asname_table = """ CREATE TABLE IF NOT EXISTS asn_asname(
                                        asn integer,
                                        asn_name text,
                                        source text,
                                        asof_date date
                                    ); """

sql_create_asn_loc_table = """ CREATE TABLE IF NOT EXISTS asn_loc(
                                        asn integer,
                                        latitude numeric,
                                        longitude numeric,
                                        source text,
                                        validated text,
                                        standard_latitude numeric,
                                        standard_longitude numeric,
                                        standard_city text,
                                        standard_state text,
                                        standard_country text,
                                        physical_presence text,
                                        asof_date date
                                    ); """

sql_create_asn_org_table = """ CREATE TABLE IF NOT EXISTS asn_org(
                                        asn integer,
                                        organization text,
                                        source text,
                                        asof_date date
                                    ); """


sql_create_asn_conn_table = """ CREATE TABLE IF NOT EXISTS asn_conn(
                                        relationship_type text,
                                        asn1 integer,
                                        asn2 integer,
                                        source text,
                                        asof_date date
                                    ); """

sql_create_nodes_table = """ CREATE TABLE IF NOT EXISTS phys_nodes(
                                        organization text,
                                        node_name text,
                                        latitude numeric,
                                        longitude numeric,
                                        city text,
                                        state text,
                                        country text,
                                        source text,
                                        asof_date date
                                    ); """

sql_create_standard_paths_table = """ CREATE TABLE IF NOT EXISTS standard_paths(
                                        from_city text,
                                        from_state text,
                                        from_country text,
                                        to_city text,
                                        to_state text,
                                        to_country text,
                                        distance_km numeric,
                                        path_wkt text,
                                        asof_date date
                                    ); """

sql_create_submarine_cables_table = """ CREATE TABLE IF NOT EXISTS submarine_cables(
                                        cable_id text,
                                        cable_name text,
                                        feature_id text,
                                        cable_wkt text,
                                        source text,
                                        asof_date date
                                    ); """

sql_create_landing_points_table = """ CREATE TABLE IF NOT EXISTS landing_points(
                                        city_name text,
                                        state_province text,
                                        country text,
                                        latitude numeric,
                                        longitude numeric,
                                        standard_city text,
                                        standard_state text,
                                        standard_country text,
                                        source text,
                                        asof_date date
                                    ); """

sql_create_cable_landing_points_table = """ CREATE TABLE IF NOT EXISTS cable_landing_points(
                                        cable_id text,
                                        city_name text,
                                        state_province text,
                                        country text,
                                        active text,
                                        source text,
                                        asof_date date
                                    ); """

tables = {
        'city_points':sql_create_city_points_table,
        'city_polygons':sql_create_city_polygons_table,
        'ip_asn_dns':sql_create_ip_asn_dns_table,
        'ip_dns':sql_create_ip_dns_table,
        'ip_inference':sql_create_ip_inference_table,
        'traceroutes':sql_create_traceroutes_table,
        'asn_asname':sql_create_asn_asname_table,
        'asn_loc':sql_create_asn_loc_table,
        'asn_org':sql_create_asn_org_table,
        'asn_conn':sql_create_asn_conn_table,
        'phys_nodes':sql_create_nodes_table,
        'standard_paths':sql_create_standard_paths_table,
        'submarine_cables':sql_create_submarine_cables_table,
        'landing_points':sql_create_landing_points_table,
        'cable_landing_points':sql_create_cable_landing_points_table
}
