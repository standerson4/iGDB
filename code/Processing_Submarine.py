import os
from pathlib import Path
import geopandas as gpd
import pandas as pd
import csv
import json
import dbStructure

class ProcessingSubmarine:
    """
        This class processes the submarine cable geojson file from Telegeography.
    """
    def __init__(self, in_dir, out_dir, help_dir):
        self.in_dir = in_dir
        self.cable_data_dir = in_dir / 'cable_data'
        self.asof_date = ''
        self.out_dir = out_dir
        # first table is the geometry for the cables
        name, fields = self.read_fields(dbStructure.sql_create_submarine_cables_table)
        self.cables_header = fields
        self.cables_table = name 

        self.cables_list = []
        self.data_source = "Telegeography"

        if not os.path.isdir(self.out_dir / self.cables_table):
            os.makedirs(self.out_dir / self.cables_table)

        # this table has the geometry for the landing points
        name, fields = self.read_fields(dbStructure.sql_create_landing_points_table)
        self.landing_header = fields
        self.landing_table = name 

        self.landing_df = pd.DataFrame()

        if not os.path.isdir(self.out_dir / self.landing_table):
            os.makedirs(self.out_dir / self.landing_table)

        self.shapefile = help_dir / 'cities_Voronoi.shp'

        # this table has the landing points for each cable
        name, fields = self.read_fields(dbStructure.sql_create_cable_landing_points_table)
        self.cable_landing_header = fields
        self.cable_landing_table = name 

        self.cable_landing_list = []

        if not os.path.isdir(self.out_dir / self.cable_landing_table):
            os.makedirs(self.out_dir / self.cable_landing_table)

    def read_fields(self, sql_str):
        """Reads in the table name and field names from the dbStructure file.
        The dbStructure file should be the standard for the DB,
        and everything should reference it for the ground truth."""
        table_fields = []
        table_name = sql_str.split('\n')[0].rstrip().split(' ')[-1].replace('(', '')
        sql_list = sql_str.split('\n')[1:-1]
        for row in sql_list:
            field = row.lstrip().split(' ')[0].upper()
            if 'PRIMARY' in field or 'FOREIGN' in field:
                continue
            table_fields.append(field)
        return table_name, table_fields

    def run_steps(self):
        print("Processing local data from Telegeography.")
        if not os.path.isfile(self.shapefile):
            print("\tThe Voronoi map helper file does not exist. Cannot process.")
            return
        cable_dumps = []
        landing_dumps = []
        for f in os.listdir(self.in_dir):
            if 'cable' in f and 'json' in f:
                cable_dumps.append(f)
            if 'landing' in f and 'json' in f:
                landing_dumps.append(f)

        # process the cable linestring data
        for c in cable_dumps:
            year = c.split('_')[1]
            month = c.split('_')[2]
            day = c.split('_')[3].replace('.json', '')
            self.asof_date = f"{year}-{month}-{day}"
            self.process_cables(self.in_dir / c)
        cables_save_file = self.out_dir / self.cables_table / f"{self.data_source}_{self.cables_table}.csv"
        self.save_csv(self.cables_list, self.cables_header, cables_save_file)


        # process the landing points data
        for ld in landing_dumps:
            year = ld.split('_')[1]
            month = ld.split('_')[2]
            day = ld.split('_')[3].replace('.json', '')
            self.asof_date = f"{year}-{month}-{day}"
            self.process_landing(self.in_dir / ld, self.shapefile)
        save_file = self.out_dir / self.landing_table / f"{self.data_source}_{self.landing_table}.csv"
        self.landing_df.to_csv(save_file, index=False)

        # process the landing points for each cable
        self.process_cable_landing()
        save_file = self.out_dir / self.cable_landing_table / f"{self.data_source}_{self.cable_landing_table}.csv"
        self.save_csv(self.cable_landing_list, self.cable_landing_header, save_file)

    def process_cables(self, f_name):
        cables_df = gpd.read_file(f_name)
        for i, c in cables_df.iterrows():
            cable_id = c["id"]
            cable_name = c["name"]
            feat_id = c["feature_id"]
            geom = c["geometry"]
            self.cables_list.append([cable_id, cable_name, feat_id, geom.wkt,
                self.data_source, self.asof_date])

    def process_landing(self, f_name, vor_fname):
        tele_df = gpd.read_file(f_name)
        landing_dict = {}
        landing_dict['city_name'] = []
        landing_dict['state_province'] = []
        landing_dict['country'] = []
        landing_dict['latitude'] = []
        landing_dict['longitude'] = []
        landing_dict['source'] = []
        landing_dict['asof_date'] = []
        for i, lp in tele_df.iterrows():
            csc = lp['name'].split(',')
            if len(csc) == 2:
                city_name = csc[0]
                state = ''
                cc = csc[1]
            elif len(csc) == 3:
                city_name = csc[0]
                state = csc[1]
                cc = csc[2]
            elif len(csc) == 4:
                city_name = csc[0]
                state = csc[2]
                cc = csc[3]
            else:
                print(lp['name'])
                print(csc)
                input("Error with city name. Enter to skip it.")
                continue
            lon = lp['geometry'].x
            lat = lp['geometry'].y
            landing_dict['city_name'].append(city_name)
            landing_dict['state_province'].append(state)
            landing_dict['country'].append(cc)
            landing_dict['latitude'].append(lat)
            landing_dict['longitude'].append(lon)
            landing_dict['source'].append(self.data_source)
            landing_dict['asof_date'].append(self.asof_date)

        # add the standard city names
        l_df = gpd.GeoDataFrame(landing_dict,
                geometry=gpd.points_from_xy(landing_dict['longitude'], landing_dict['latitude'],
                    crs="EPSG:4326"))
        all_df = gpd.read_file(vor_fname)
        loc_df = all_df[["NAME", "ADM1NAME", "ISO_A2", "geometry"]]
        join_df = gpd.sjoin(l_df, loc_df, how="inner", predicate='within')
        landing_df = join_df[["city_name", "state_province", "country", "latitude", "longitude",
            "source", "asof_date", "NAME", "ADM1NAME", "ISO_A2"]]
        # format the columns and data for entry to the DB
        if self.landing_df.empty:
            self.landing_df = landing_df.rename(columns={"NAME":"standard_city",
                "ADM1NAME":"standard_state", "ISO_A2":"standard_country"})
        else:
            landing_df = landing_df.rename(columns={"NAME":"standard_city",
                "ADM1NAME":"standard_state", "ISO_A2":"standard_country"})
            self.landing_df = pd.concat([self.landing_df, landing_df])
        self.landing_df['city_name'] = self.landing_df.city_name.str.replace("'", "''")
        self.landing_df['state_province'] = self.landing_df.state_province.str.replace("'", "''")
        self.landing_df['country'] = self.landing_df.country.str.replace("'", "''")
        self.landing_df['standard_city'] = self.landing_df.standard_city.str.replace("'", "''")
        self.landing_df['standard_state'] = self.landing_df.standard_state.str.replace("'", "''")
        self.landing_df['standard_country'] = self.landing_df.standard_country.str.replace("'", "''")

    def process_cable_landing(self):
        for f_name in os.listdir(self.cable_data_dir):
            with open(self.cable_data_dir / f_name, 'r') as f:
                d = json.load(f)
            c_id = f_name.split('_')[0]
            year = f_name.split('_')[1]
            month = f_name.split('_')[2]
            day = f_name.split('_')[3].replace('.json', '')
            asof_date = f"{year}-{month}-{day}"
            for lp in d['landing_points']:
                csc = lp['name'].split(',')
                if len(csc) == 2:
                    city = csc[0].replace("'", "''")
                    state = ''
                    country= csc[1].strip().replace("'", "''")
                elif len(csc) == 3:
                    city = csc[0].replace("'", "''")
                    state = csc[1].replace("'", "''")
                    country= csc[2].strip().replace("'", "''")
                elif len(csc) == 4:
                    city = csc[0].replace("'", "''")
                    state = csc[1].replace("'", "''")
                    country= csc[3].strip().replace("'", "''")
                else:
                    print(f"Error: {csc}")
                    input()

                #print(type(lp['is_tbd']))
                if not lp['is_tbd']:
                    active = True
                else:
                    active = False
                r = [c_id, city, state, country, active, self.data_source, asof_date]
                #print(r)
                #input()
                self.cable_landing_list.append(r)

    def save_csv(self, data, header, f_name):
        print(f"\tSaving to {f_name}.")
        with open(f_name, 'w') as f:
            csv_writer = csv.writer(f, delimiter=',')
            csv_writer.writerow(header)
            for d in data:
                csv_writer.writerow(d)

if __name__ == "__main__":
    print("This script should not be run by itself. Run it through iGDB.py")
    input_dir = Path("../unprocessed/Telegeography")
    output_dir = Path("../processed")
    help_dir = Path("../helper_data/cities_Voronoi")
    my_processor = ProcessingSubmarine(input_dir, output_dir, help_dir)
    my_processor.run_steps()
