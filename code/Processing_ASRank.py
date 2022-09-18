from pathlib import Path
import os
import json
import csv
from datetime import date
from datetime import timedelta
import dbStructure

class ProcessingASRank:
    def __init__(self, in_dir, out_dir):
        self.in_dir = in_dir
        self.links_file = 'LINKS.json'
        self.asn_asname_file = 'ASNS.json'
        self.orgs_file = 'ORGS.json'
        self.asof_date = ''
        self.data_source = "ASRank"
        if not os.path.isdir(self.in_dir):
            print(f"{self.in_dir} does not exist.")
            return
        #self.find_nearest_input_files(in_dir)
        self.out_dir =  out_dir
        self.org_map = {}

        # setup for asn_conn table
        self.asn_conn_list = []
        name, fields = self.read_fields(dbStructure.sql_create_asn_conn_table)
        self.asn_conn_header = fields
        self.asn_conn_table = name 

        if not os.path.isdir(self.out_dir / self.asn_conn_table):
            os.makedirs(self.out_dir / self.asn_conn_table)

        # setup for asn_asname table
        self.asn_asname_list = []
        name, fields = self.read_fields(dbStructure.sql_create_asn_asname_table)
        self.asn_asname_header = fields
        self.asn_asname_table = name 

        if not os.path.isdir(self.out_dir / self.asn_asname_table):
            os.makedirs(self.out_dir / self.asn_asname_table)

        # setup for asn_org table
        self.asn_org_list = []
        name, fields = self.read_fields(dbStructure.sql_create_asn_org_table)
        self.asn_org_header = fields
        self.asn_org_table = name 

        if not os.path.isdir(self.out_dir / self.asn_org_table):
            os.makedirs(self.out_dir / self.asn_org_table)

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
        print("Processing local data from CAIDA ASRank.")
        if not os.path.isdir(self.in_dir):
            print("\tThere is no data to process. Update the CAIDA ASRank data before continuing.")
            return

        for folder in os.listdir(self.in_dir):
            self.asof_date = folder.replace("_", "-")
            self.read_links_file(self.in_dir / folder / self.links_file)
            self.read_orgs_file(self.in_dir / folder / self.orgs_file)
            self.read_asns_file(self.in_dir / folder / self.asn_asname_file)

        t_file = f"{self.data_source}_{self.asn_conn_table}.csv" 
        asn_conn_file = self.out_dir / self.asn_conn_table / t_file
        self.save_csv(self.asn_conn_list, self.asn_conn_header, asn_conn_file)

        t_file = f"{self.data_source}_{self.asn_asname_table}.csv"
        asn_asname_file = self.out_dir / self.asn_asname_table / t_file
        self.save_csv(self.asn_asname_list, self.asn_asname_header, asn_asname_file)

        t_file = f"{self.data_source}_{self.asn_org_table}.csv"
        asn_org_file = self.out_dir / self.asn_org_table / t_file
        self.save_csv(self.asn_org_list, self.asn_org_header, asn_org_file)

    def read_links_file(self, f_name):
        print(f"\tReading {f_name}")
        link_list = []
        with open(f_name, 'r') as f:
            link_list = json.load(f)

        for row in link_list:
            node = row['node']
            rel = node['relationship']
            asn1 = int(node['asn0']['asn'])
            asn2 = int(node['asn1']['asn'])
            new_row = [rel, asn1, asn2, self.data_source, self.asof_date]
            self.asn_conn_list.append(new_row)

    def read_orgs_file(self, f_name):
        print(f"\tReading {f_name}")
        orgs_list = []
        with open(f_name, 'r') as f:
            orgs_list = json.load(f)
        for o in orgs_list:
            org_id = o["node"]["orgId"]
            org_name = o["node"]["orgName"].replace("'", "''")
            self.org_map[org_id] = org_name

    def read_asns_file(self, f_name):
        """
        This json file populates two relations: asn_asname and asn_org.
        But, organizations are listed by ID and not name, so we need to
        read in the organization names from another source (ORGS file).
        """
        print(f"\tReading {f_name}")
        asn_list = []
        with open(f_name, 'r') as f:
            asn_list = json.load(f)
        for a in asn_list:
            asn = a["node"]["asn"]
            asn_name = a["node"]["asnName"].replace("'", "''")
            if a["node"]["organization"]:
                org_id = a["node"]["organization"]["orgId"]
            else:
                org_id = None
            if asn_name:
                new_row = [asn, asn_name, self.data_source, self.asof_date]
                self.asn_asname_list.append(new_row)
            if org_id:
                org_name = self.org_map[org_id]
                new_row = [asn, org_name, self.data_source, self.asof_date]
                self.asn_org_list.append(new_row)

    def save_csv(self, data, header, f_name):
        print(f"\tSaving to {f_name}.")
        with open(f_name, 'w') as f:
            csv_writer = csv.writer(f, delimiter=',')
            csv_writer.writerow(header)
            for d in data:
                csv_writer.writerow(d)

if __name__ == "__main__":
    print("This script should not be run by itself. Run it through iGDB.py")
    input_dir = Path("../unprocessed/ASRank")
    output_dir = Path("../processed")
    my_processor = ProcessingASRank(input_dir, output_dir)
    my_processor.run_steps()

