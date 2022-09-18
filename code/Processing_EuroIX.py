from pathlib import Path
import os
import json
import csv
from datetime import date
from datetime import timedelta
import dbStructure

class ProcessingEuroIX:
    def __init__(self, in_dir, out_dir):
        self.in_dir = in_dir
        self.asns_file = 'ASNS.json'
        self.asn_switch_file = 'ASNS-BY-IXP-SWITCH.json'
        self.ixps_file = 'IXPS.json'
        self.asof_date = ''
        self.data_source = "EuroIX"
        if not os.path.isdir(self.in_dir):
            print(f"{self.in_dir} does not exist.")
            return
        self.out_dir =  out_dir
        self.ixp_map = {}

        # setup for asn_asname table
        self.asn_asname_list = []
        name, fields = self.read_fields(dbStructure.sql_create_asn_asname_table)
        self.asn_asname_header = fields
        self.asn_asname_table = name 

        if not os.path.isdir(self.out_dir / self.asn_asname_table):
            os.makedirs(self.out_dir / self.asn_asname_table)

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
        print("Processing local data from EuroIX.")
        if not os.path.isdir(self.in_dir):
            print("\tThere is no data to process. Update the CAIDA ASRank data before continuing.")
            return

        for folder in os.listdir(self.in_dir):
            self.asof_date = folder.replace("_", "-")
            self.read_ixps_file(self.in_dir / folder / self.ixps_file)
            self.read_asns_file(self.in_dir / folder / self.asns_file)

        t_file = f"{self.data_source}_{self.asn_asname_table}.csv"
        asn_asname_file = self.out_dir / self.asn_asname_table / t_file
        self.save_csv(self.asn_asname_list, self.asn_asname_header, asn_asname_file)

    def read_ixps_file(self, f_name):
        print(f"\tReading {f_name}")
        ixp_list = []
        with open(f_name, 'r') as f:
            ixp_list = json.load(f)

        for row in ixp_list:
            fac_id = row['fields']['ixp_number_ix_f_id']
            name = row['fields']['name']
            if 'metro' in row['fields'].keys():
                loc = row['fields']['metro'].replace("'", "''")
            else:
                loc = ''
            if 'ipv4' in row['fields'].keys():
                ipv4_sub = row['fields']['ipv4']
            else:
                ipv4_sub = ''
            if 'ipv6' in row['fields'].keys():
                ipv6_sub = row['fields']['ipv6']
            else:
                ipv6_sub = ''
            if 'coordinates' in row['fields'].keys():
                lat = float(row['fields']['coordinates'][0])
                lon = float(row['fields']['coordinates'][1])
            else:
                lat = None
                lon = None
            if 'switch_name' in row['fields'].keys():
                sw_name = row['fields']['switch_name']
            else:
                sw_name = ''
            if 'switch_model' in row['fields'].keys():
                sw_model = row['fields']['switch_model']
            else:
                sw_model = ''
            if 'ixp_switch' in row['fields'].keys():
                ixp_sw = row['fields']['ixp_switch']
            else:
                ixp_sw = ''

            if not fac_id in self.ixp_map.keys():
                self.ixp_map[fac_id] = {}
                self.ixp_map[fac_id]["name"] = name
                self.ixp_map[fac_id]["location"] = []
                self.ixp_map[fac_id]["IPv4"] = ipv4_sub
                self.ixp_map[fac_id]["IPv6"] = ipv6_sub
            loc_row = (sw_name, sw_model, ixp_sw, loc, lat, lon)
            if not loc_row in self.ixp_map[fac_id]["location"]:
                self.ixp_map[fac_id]["location"].append(loc_row)

    def read_asns_file(self, f_name):
        print(f"\tReading {f_name}")
        asn_list = []
        with open(f_name, 'r') as f:
            asn_list = json.load(f)
        for row in asn_list:
            asn = row['fields']['asn']
            asn_name = row['fields']['name'].replace("'", "''")
            fac_id = row['fields']['ixp_number_ix_f_id']
            if 'ipv4_address' in row['fields'].keys():
                ipv4_addr = row['fields']['ipv4_address']
            else:
                ipv4_addr = None
            if 'ipv6_address' in row['fields'].keys():
                ipv6_addr = row['fields']['ipv6_address']
            else:
                ipv6_addr = None
            if 'switch_name' in row['fields'].keys():
                sw_name = row['fields']['switch_name']
            else:
                sw_name = ''
            if 'switch_model' in row['fields'].keys():
                sw_model = row['fields']['switch_model']
            else:
                sw_model = ''
            if 'ixp_switch' in row['fields'].keys():
                ixp_sw = row['fields']['ixp_switch']
            else:
                ixp_sw = ''
            new_row = [asn, asn_name, self.data_source, self.asof_date]
            if not new_row in self.asn_asname_list:
                self.asn_asname_list.append(new_row)

    def save_csv(self, data, header, f_name):
        print(f"\tSaving to {f_name}.")
        with open(f_name, 'w') as f:
            csv_writer = csv.writer(f, delimiter=',')
            csv_writer.writerow(header)
            for d in data:
                csv_writer.writerow(d)


if __name__ == "__main__":
    print("This script should not be run by itself. Run it through iGDB.py")
    input_dir = Path("../unprocessed/EuroIX")
    output_dir = Path("../processed")
    my_processor = ProcessingEuroIX(input_dir, output_dir)
    my_processor.run_steps()

