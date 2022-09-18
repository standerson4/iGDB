from pathlib import Path
import os
import json
import csv
from datetime import date
from datetime import timedelta
import Standardize_Locations
import dbStructure

class ProcessingRIPEAtlas:
    def __init__(self, in_dir, out_dir):
        self.in_dir = in_dir
        self.asof_date = ''
        if not os.path.isdir(in_dir):
            print(f"{in_dir} does not exist.")
            return
        self.out_dir =  out_dir
        self.asn_loc_list = []
        self.physical_presence = False
        name, fields = self.read_fields(dbStructure.sql_create_asn_loc_table)
        self.asn_loc_header = fields
        self.asn_loc_table = name 

        self.data_source = "RIPEAtlas"
        voronoi_dir = Path("../helper_data/cities_Voronoi")
        if not os.path.isdir(voronoi_dir):
            print("The Voronoi map helper file does not exist. Cannot standardize city names.")
            return
        self.loc_standardizer = Standardize_Locations.LocationStandardizer(voronoi_dir)

        if not os.path.isdir(self.out_dir / self.asn_loc_table):
            os.makedirs(self.out_dir / self.asn_loc_table)

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
        print("Processing local data from RIPE Atlas. ", end='')
        if not os.path.isdir(self.in_dir):
            print("\n\tThere is no data to process. Update the RIPE Atlas data before continuing.")
            return
        print("\tThis takes a while. Status reported every 10 files.")
        for d in os.listdir(self.in_dir):
            year = d.split('_')[0]
            month = d.split('_')[1]
            day = d.split('_')[2]
            self.asof_date = f"{year}-{month}-{day}"

            for i, f in enumerate(os.listdir(self.in_dir / d)):
                if (i+1) % 10 == 0:
                    print(f"\tWorking on file {i+1} of {len(os.listdir(self.in_dir / d))}.")
                if 'anchor' in f:
                    self.read_anchor_file(self.in_dir / d / f)
                elif 'probes' in f:
                    self.read_probe_file(self.in_dir / d / f)

        asn_loc_file = self.out_dir / self.asn_loc_table / f"{self.data_source}_{self.asn_loc_table}.csv"
        self.save_csv(self.asn_loc_list, self.asn_loc_header, asn_loc_file)

    def read_anchor_file(self, f_name):
        validated = ''
        with open(f_name, 'r') as f:
            raw_data = json.load(f)
        if 'results' in raw_data.keys():
            for r in raw_data['results']:
                try:
                    city = r['city'].split(',')[0]
                    country = r['country']
                    as_v4 = r['as_v4']
                    lat = round(float(r['geometry']['coordinates'][1]), 4)
                    lon = round(float(r['geometry']['coordinates'][0]), 4)
                except:
                    continue
                if not as_v4:
                    continue
                std_loc = {}
                std_loc = self.loc_standardizer.standardize([lat, lon])
                if std_loc:
                    std_lat = std_loc["LATITUDE"]
                    std_lon = std_loc["LONGITUDE"]
                    std_city = std_loc["CITY"].replace("'", "''")
                    try:
                        std_state = std_loc["STATE"].replace("'", "''")
                    except:
                        std_state = None
                    std_country = std_loc["COUNTRY"]
                else:
                    std_lat = 'NULL'
                    std_lon = 'NULL'
                    std_city = 'NULL'
                    std_state = 'NULL'
                    std_country = 'NULL'

                new_row = [as_v4, lat, lon, self.data_source, validated,
                        std_lat, std_lon, std_city, std_state, std_country,
                        self.physical_presence, self.asof_date]
                #if not new_row in self.city_asn_list:
                if not new_row in self.asn_loc_list:
                    self.asn_loc_list.append(new_row)

    def read_probe_file(self, f_name):
        validated = ''
        with open(f_name, 'r') as f:
            raw_data = json.load(f)
        if 'results' in raw_data.keys():
            for r in raw_data['results']:
                try:
                    country = r['country_code']
                    as_v4 = r['asn_v4']
                    lat = round(float(r['geometry']['coordinates'][1]), 4)
                    lon = round(float(r['geometry']['coordinates'][0]), 4)
                    status = r['status']['name']
                except:
                    continue
                if not as_v4:
                    continue
                if status == 'Connected':
                    std_loc = {}
                    if lat and lon:
                        std_loc = self.loc_standardizer.standardize([lat, lon])
                    if std_loc:
                        std_lat = std_loc["LATITUDE"]
                        std_lon = std_loc["LONGITUDE"]
                        std_city = std_loc["CITY"].replace("'", "''")
                        try:
                            std_state = std_loc["STATE"].replace("'", "''")
                        except:
                            std_state = None
                        std_country = std_loc["COUNTRY"]
                    else:
                        std_lat = 'NULL'
                        std_lon = 'NULL'
                        std_city = 'NULL'
                        std_state = 'NULL'
                        std_country = 'NULL'
                    new_row = [as_v4, lat, lon, self.data_source, validated,
                            std_lat, std_lon, std_city, std_state, std_country,
                            self.physical_presence, self.asof_date]
                    if not new_row in self.asn_loc_list:
                        self.asn_loc_list.append(new_row)

    def save_csv(self, data, header, f_name):
        print(f"\tSaving to {f_name}.")
        with open(f_name, 'w') as f:
            csv_writer = csv.writer(f, delimiter=',')
            csv_writer.writerow(header)
            for d in data:
                csv_writer.writerow(d)

if __name__ == "__main__":
    print("This script should not be run by itself. Run it through iGDB.py")
    input_dir = Path("../unprocessed/RIPEAtlas")
    output_dir = Path("../processed")
    my_processor = ProcessingRIPEAtlas(input_dir, output_dir)
    my_processor.run_steps()

