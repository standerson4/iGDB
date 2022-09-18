import os
from pathlib import Path
import json
import csv
from datetime import date
from datetime import timedelta
import Standardize_Locations
import dbStructure

class ProcessingPCH:
    """
        This class processes the IXPs and ASes at each IXP from PCH
        to identify the ASes at each location.
    """
    def __init__(self, in_dir, out_dir):
        self.in_dir = ''
        self.asof_date = ''
        if not os.path.isdir(in_dir):
            print(f"{in_dir} does not exist.")
            return
        self.find_nearest_input_folder(in_dir)
        self.out_dir = out_dir
        self.ixp_loc_dict = {}
        self.asn_loc_dict = {}
        self.asn_loc_list = []
        self.physical_presence = False
        name, fields = self.read_fields(dbStructure.sql_create_asn_loc_table)
        self.asn_loc_header = fields
        self.asn_loc_table = name 

        self.asn_org_list = []
        name, fields = self.read_fields(dbStructure.sql_create_asn_org_table)
        self.asn_org_header = fields
        self.asn_org_table = name

        self.ip_asn_list = []
        name, fields = self.read_fields(dbStructure.sql_create_ip_asn_dns_table)
        self.ip_asn_header = fields
        self.ip_asn_table = name

        self.data_source = "PCH"
        voronoi_dir = Path("../helper_data/cities_Voronoi")
        if not os.path.isdir(voronoi_dir):
            print("The Voronoi map helper file does not exist. Cannot standardize city names.")
            return
        self.loc_standardizer = Standardize_Locations.LocationStandardizer(voronoi_dir)

        if not os.path.isdir(self.out_dir / self.asn_loc_table):
            os.makedirs(self.out_dir / self.asn_loc_table)

        if not os.path.isdir(self.out_dir / self.asn_org_table):
            os.makedirs(self.out_dir / self.asn_org_table)

        if not os.path.isdir(self.out_dir / self.ip_asn_table):
            os.makedirs(self.out_dir / self.ip_asn_table)

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
        print("Processing local data from Packet Clearinghouse. ", end='')
        if not os.path.isdir(self.in_dir):
            print("\n\tThere is no data to process. Update the PCH data before continuing.")
            return
        print("This takes a while. Status reported every 100 IXPs.")
        self.read_ixp_file(self.in_dir / "pch_active_ixp.json")
        self.read_subnets_file(self.in_dir / "pch_subnets.json")
        asn_loc_file = self.out_dir / self.asn_loc_table / f"{self.data_source}_{self.asn_loc_table}.csv"
        self.save_csv(self.asn_loc_list, self.asn_loc_header, asn_loc_file)

        ip_asn_file = self.out_dir / self.ip_asn_table / f"{self.data_source}_{self.ip_asn_table}.csv"
        self.save_csv(self.ip_asn_list, self.ip_asn_header, ip_asn_file)

        asn_org_file = self.out_dir / self.asn_org_table / f"{self.data_source}_{self.asn_org_table}.csv"
        self.save_csv(self.asn_org_list, self.asn_org_header, asn_org_file)

    def find_nearest_input_folder(self, start_dir):
        today = date.today()
        min_time = 100
        for f in os.listdir(start_dir):
            if not os.path.isdir(start_dir / f):
                continue
            try:
                year = int(f.split('_')[0])
                month_str = f.split('_')[1]
                if month_str[0] == '0':
                    month = int(month_str[-1])
                else:
                    month = int(month_str)
                day_str = f.split('_')[2]
                if day_str[0] == '0':
                    day = int(day_str[-1])
                else:
                    day = int(day_str)
                f_date = date(year, month, day)
                num_days = (today - f_date).days
                if num_days < min_time:
                    min_time = num_days
                    self.in_dir = start_dir / f
                    self.asof_date = f"{year}-{month_str}-{day_str}"
            except:
                continue

    def read_ixp_file(self, ixp_file):
        ixp_list = self.read_json(ixp_file)
        for i, ixp in enumerate(ixp_list):
            if (i+1) % 100 == 0:
                print(f"\tWorking on IXP {i+1} of {len(ixp_list)}.")
            ixp_id = ixp['id']
            city = ixp['cit']
            country = ixp['ctry']
            ixp_name = ixp['name']
            if ixp['lat']:
                lat = round(float(ixp['lat']), 4)
            else:
                lat = ''
            if ixp['lon']:
                lon = round(float(ixp['lon']), 4)
            else:
                lon = ''
            self.ixp_loc_dict[ixp_id] = {}
            self.ixp_loc_dict[ixp_id]["CITY"] = city
            self.ixp_loc_dict[ixp_id]["COUNTRY"] = country
            self.ixp_loc_dict[ixp_id]["IXP_NAME"] = ixp_name
            self.ixp_loc_dict[ixp_id]["LATITUDE"] = lat
            self.ixp_loc_dict[ixp_id]["LONGITUDE"] = lon

            if lat and lon:
                std_loc = self.loc_standardizer.standardize([lat, lon])
                self.ixp_loc_dict[ixp_id]["STD_LATITUDE"] = std_loc["LATITUDE"]
                self.ixp_loc_dict[ixp_id]["STD_LONGITUDE"] = std_loc["LONGITUDE"]
                self.ixp_loc_dict[ixp_id]["STD_CITY"] = std_loc["CITY"].replace("'", "''")
                try:
                    self.ixp_loc_dict[ixp_id]["STD_STATE"] = std_loc["STATE"].replace("'", "''")
                except:
                    self.ixp_loc_dict[ixp_id]["STD_STATE"] = None
                self.ixp_loc_dict[ixp_id]["STD_COUNTRY"] = std_loc["COUNTRY"]
            else:
                self.ixp_loc_dict[ixp_id]["STD_LATITUDE"] = 'NULL'
                self.ixp_loc_dict[ixp_id]["STD_LONGITUDE"] = 'NULL'
                self.ixp_loc_dict[ixp_id]["STD_CITY"] = 'NULL'
                self.ixp_loc_dict[ixp_id]["STD_STATE"] = 'NULL'
                self.ixp_loc_dict[ixp_id]["STD_COUNTRY"] = 'NULL'

    def read_subnets_file(self, subnets_file):
        validated_status = ''
        subnets_dict = self.read_json(subnets_file)
        asn_org_dict = {}
        for ixp_id in subnets_dict.keys():
            if isinstance(subnets_dict[ixp_id], list):
                continue
            for ip_ver in ['IPv4', 'IPv6']:
                if ip_ver in subnets_dict[ixp_id].keys():
                    for ip_range in subnets_dict[ixp_id][ip_ver].keys():
                        for ip in subnets_dict[ixp_id][ip_ver][ip_range].keys():
                            ip_dict = subnets_dict[ixp_id][ip_ver][ip_range][ip]
                            asn = ip_dict['asn']
                            org_name = ip_dict['org'].replace("'", "''")
                            ip_addr = ip_dict['ip']
                            rdns = ip_dict['fqdn']
                            if not asn:
                                continue
                            try:
                                asn = int(asn)
                            except:
                                print(f"\tCould not process {asn}.")
                                continue
                            if not int(asn) in asn_org_dict.keys():
                                asn_org_dict[int(asn)] = org_name
                            city = self.ixp_loc_dict[ixp_id]["CITY"]
                            country = self.ixp_loc_dict[ixp_id]["COUNTRY"]
                            lat = self.ixp_loc_dict[ixp_id]["LATITUDE"]
                            lon = self.ixp_loc_dict[ixp_id]["LONGITUDE"]

                            std_lat = self.ixp_loc_dict[ixp_id]["STD_LATITUDE"]
                            std_lon = self.ixp_loc_dict[ixp_id]["STD_LONGITUDE"]
                            std_city = self.ixp_loc_dict[ixp_id]["STD_CITY"]
                            std_state = self.ixp_loc_dict[ixp_id]["STD_STATE"]
                            std_country = self.ixp_loc_dict[ixp_id]["STD_COUNTRY"]

                            loc_row = (asn, lat, lon, self.data_source,
                                    validated_status, std_lat, std_lon,
                                    std_city, std_state, std_country,
                                    self.physical_presence, self.asof_date)
                            if lat and lon:
                                if not loc_row in self.asn_loc_list:
                                    self.asn_loc_list.append(loc_row)
                            ip_row = (ip_addr, rdns, asn, std_city, std_state,
                                    std_country, self.data_source, self.asof_date)
                            if lat and lon:
                                if not ip_row in self.ip_asn_list:
                                    self.ip_asn_list.append(ip_row)

        for asn in asn_org_dict.keys():
            row = [asn, asn_org_dict[asn], self.data_source, self.asof_date]
            self.asn_org_list.append(row)

    def read_json(self, f_name):
        data = {}
        with open(f_name, 'r') as f:
            data = json.load(f)
        return data


    def save_csv(self, data, header, f_name):
        print(f"\tSaving to {f_name}.")
        with open(f_name, 'w') as f:
            csv_writer = csv.writer(f, delimiter=',')
            csv_writer.writerow(header)
            for d in data:
                csv_writer.writerow(d)

if __name__ == "__main__":
    print("This script should not be run by itself. Run it through iGDB.py")
    input_dir = Path("../unprocessed/PCH")
    output_dir = Path("../processed")
    my_processor = ProcessingPCH(input_dir, output_dir)
    my_processor.run_steps()
