import os
from pathlib import Path
import json
import csv
import Standardize_Locations
import dbStructure

class ProcessingPDB:
    """
        This class processes the IXPs and ASes at each IXP from PDB
        to identify the ASes at each location.
    """
    def __init__(self, in_dir, out_dir):
        self.in_dir = in_dir
        self.asof_date = ''
        self.out_dir = out_dir
        self.pdb_dict = {}
        self.fac_loc_dict = {}
        self.asn_loc_list = []
        name, fields = self.read_fields(dbStructure.sql_create_asn_loc_table)
        self.asn_loc_header = fields
        self.asn_loc_table = name 

        self.asn_org_list = []
        name, fields = self.read_fields(dbStructure.sql_create_asn_org_table)
        self.asn_org_header = fields
        self.asn_org_table = name

        self.asn_asname_list = []
        name, fields = self.read_fields(dbStructure.sql_create_asn_asname_table)
        self.asn_asname_header = fields
        self.asn_asname_table = name

        self.phys_nodes_list = []
        name, fields = self.read_fields(dbStructure.sql_create_nodes_table)
        self.phys_nodes_header = fields
        self.phys_nodes_table = name

        self.data_source = "PeeringDB"
        voronoi_dir = Path("../helper_data/cities_Voronoi")
        if not os.path.isdir(voronoi_dir):
            print("The Voronoi map helper file does not exist. Cannot standardize city names.")
            return
        self.loc_standardizer = Standardize_Locations.LocationStandardizer(voronoi_dir)

        if not os.path.isdir(self.out_dir / self.asn_loc_table):
            os.makedirs(self.out_dir / self.asn_loc_table)

        if not os.path.isdir(self.out_dir / self.asn_org_table):
            os.makedirs(self.out_dir / self.asn_org_table)

        if not os.path.isdir(self.out_dir / self.asn_asname_table):
            os.makedirs(self.out_dir / self.asn_asname_table)

        if not os.path.isdir(self.out_dir / self.phys_nodes_table):
            os.makedirs(self.out_dir / self.phys_nodes_table)

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
        print("Processing local data from PeeringDB.")
        if not os.path.isdir(self.in_dir):
            print("\tThere is no data to process. Update the PeeringDB data before continuing.")
            return
        all_dumps = []
        for f in os.listdir(self.in_dir):
            if 'json' in f:
                all_dumps.append(f)

        for d in all_dumps:
            year = d.split('_')[3]
            month = d.split('_')[4]
            day = d.split('_')[5].replace('.json', '')
            self.asof_date = f"{year}-{month}-{day}"
            self.pdb_dict = self.read_json(self.in_dir / d)
            self.process_asn_orgs()
            self.process_asn_locs()

        asn_org_file = self.out_dir / self.asn_org_table / f"{self.data_source}_{self.asn_org_table}.csv"
        self.save_csv(self.asn_org_list, self.asn_org_header, asn_org_file)

        asn_loc_file = self.out_dir / self.asn_loc_table / f"{self.data_source}_{self.asn_loc_table}.csv"
        self.save_csv(self.asn_loc_list, self.asn_loc_header, asn_loc_file)

        t_name = f"{self.data_source}_{self.asn_asname_table}.csv"
        asn_asname_file = self.out_dir / self.asn_asname_table / t_name
        self.save_csv(self.asn_asname_list, self.asn_asname_header, asn_asname_file)

        t_name = f"{self.data_source}_{self.phys_nodes_table}.csv"
        phys_nodes_file = self.out_dir / self.phys_nodes_table / t_name
        self.save_csv(self.phys_nodes_list, self.phys_nodes_header, phys_nodes_file)

    def process_asn_orgs(self):
        print("\tWorking on the PeeringDB ASN to organization map.")
        asn_org_dict = {}
        for i, asn in enumerate(self.pdb_dict['as_set']['data'][0]):
            asn_i = int(asn)
            asn_org_dict[asn_i] = {}
            asn_org_dict[asn_i]["ASN_NAME"] = self.pdb_dict['as_set']['data'][0][asn]
            asn_org_dict[asn_i]["ORGANIZATION_NAME"] = 'NULL'
            asn_org_dict[asn_i]["ORGANIZATION_AKA"] = 'NULL'
        for i, net in enumerate(self.pdb_dict['net']['data']):
            asn = int(net['asn'])
            org_name = net['name']
            org_aka = net['aka']
            if not asn in asn_org_dict.keys():
                asn_org_dict[asn] = {}
                asn_org_dict[asn]["ASN_NAME"] = 'NULL'
            asn_org_dict[asn]["ORGANIZATION_NAME"] = org_name
            asn_org_dict[asn]["ORGANIZATION_AKA"] = org_aka
        for asn in asn_org_dict.keys():
            asn_name = asn_org_dict[asn]["ASN_NAME"].replace("'", "''")
            org_name = asn_org_dict[asn]["ORGANIZATION_NAME"].replace("'", "''")
            org_aka_unf = asn_org_dict[asn]["ORGANIZATION_AKA"].replace("'", "''")
            new_row = [asn, org_name, self.data_source, self.asof_date]
            self.asn_org_list.append(new_row)
            org_aka_list = self.split_org_aka(org_aka_unf)
            for org_aka in org_aka_list:
                if org_aka == org_name:
                    continue
                new_row = [asn, org_aka, self.data_source, self.asof_date]
                self.asn_org_list.append(new_row)

            if asn_name:
                new_row = [asn, asn_name, self.data_source, self.asof_date]
                self.asn_asname_list.append(new_row)

    def split_org_aka(self, unf_str):
        results = []
        unf_str = unf_str.replace(", LLC", " LLC")
        unf_str = unf_str.replace(", Inc", " Inc")
        unf_str = unf_str.replace(", Ltd.", " Ltd.")
        unf_str = unf_str.replace("content/video", "content & video")
        unf_str = unf_str.replace("Also/formerly known as: ", "")
        if ',' in unf_str:
            unf_list = unf_str.split(',')
        elif ';' in unf_str:
            unf_list = unf_str.split(';')
        elif '/' in unf_str:
            unf_list = unf_str.split('/')
        else:
            unf_list = [unf_str]
        for e in unf_list:
            e = e.strip()
            if e:
                results.append(e)
        return results

    def process_asn_locs(self):
        validated = ''
        print("\tWorking on the PeeringDB facilities. ", end='')
        print("This takes a while. Status reported every 100 facilities.")
        for i, fac in enumerate(self.pdb_dict['fac']['data']):
            if (i+1) % 100 == 0:
                print(f"\tWorking on facility {i+1} of {len(self.pdb_dict['fac']['data'])}.")
            fac_id = fac['id']
            if fac['latitude']:
                lat = round(float(fac['latitude']), 4)
            else:
                lat = 'NULL'
            if fac['longitude']:
                lon = round(float(fac['longitude']), 4)
            else:
                lon = 'NULL'
            self.fac_loc_dict[fac_id] = {}
            self.fac_loc_dict[fac_id]["LATITUDE"] = lat
            self.fac_loc_dict[fac_id]["LONGITUDE"] = lon
            self.fac_loc_dict[fac_id]["ORGANIZATION"] = fac['org_name'].replace("'", "''")
            self.fac_loc_dict[fac_id]["NODE_NAME"] = fac['name'].replace("'", "''")

            std_loc = {}
            if (not lat == 'NULL') and (not lon == 'NULL'):
                std_loc = self.loc_standardizer.standardize([lat, lon])
            if std_loc:
                self.fac_loc_dict[fac_id]["STD_LATITUDE"] = std_loc["LATITUDE"]
                self.fac_loc_dict[fac_id]["STD_LONGITUDE"] = std_loc["LONGITUDE"]
                self.fac_loc_dict[fac_id]["STD_CITY"] = std_loc["CITY"].replace("'", "''")
                try:
                    self.fac_loc_dict[fac_id]["STD_STATE"] = std_loc["STATE"].replace("'", "''")
                except:
                    self.fac_loc_dict[fac_id]["STD_STATE"] = None
                self.fac_loc_dict[fac_id]["STD_COUNTRY"] = std_loc["COUNTRY"]
            else:
                self.fac_loc_dict[fac_id]["STD_LATITUDE"] = 'NULL'
                self.fac_loc_dict[fac_id]["STD_LONGITUDE"] = 'NULL'
                self.fac_loc_dict[fac_id]["STD_CITY"] = 'NULL'
                self.fac_loc_dict[fac_id]["STD_STATE"] = 'NULL'
                self.fac_loc_dict[fac_id]["STD_COUNTRY"] = 'NULL'

            phys_row = [self.fac_loc_dict[fac_id]["ORGANIZATION"],
                    self.fac_loc_dict[fac_id]["NODE_NAME"],
                    self.fac_loc_dict[fac_id]["STD_LATITUDE"],
                    self.fac_loc_dict[fac_id]["STD_LONGITUDE"],
                    self.fac_loc_dict[fac_id]["STD_CITY"],
                    self.fac_loc_dict[fac_id]["STD_STATE"],
                    self.fac_loc_dict[fac_id]["STD_COUNTRY"],
                    self.data_source, self.asof_date]
            self.phys_nodes_list.append(phys_row)

        print("\tWorking on the PeeringDB networks.")
        for net in self.pdb_dict['netfac']['data']:
            asn = net['local_asn']
            fac_id = net['fac_id']
            lat = self.fac_loc_dict[fac_id]["LATITUDE"]
            lon = self.fac_loc_dict[fac_id]["LONGITUDE"]
            std_lat = self.fac_loc_dict[fac_id]["STD_LATITUDE"]
            std_lon = self.fac_loc_dict[fac_id]["STD_LONGITUDE"]
            std_city = self.fac_loc_dict[fac_id]["STD_CITY"]
            std_state = self.fac_loc_dict[fac_id]["STD_STATE"]
            std_country = self.fac_loc_dict[fac_id]["STD_COUNTRY"]
            ### adding a flag because this is a physical presence
            physical_presence = True
            new_row = [asn, lat, lon, self.data_source, validated,
                    std_lat, std_lon, std_city, std_state, std_country,
                    physical_presence, self.asof_date]
            if lat and lon:
                if not new_row in self.asn_loc_list:
                    self.asn_loc_list.append(new_row)
        print('\tWorking on the PeeringDB virtual presence')
        mapping_ixp_pop = {}
        for ixfac in self.pdb_dict['ixfac']['data']:
            mapping_ixp_pop[ixfac['ix_id']] = ixfac['fac_id']
        for net in self.pdb_dict['netixlan']['data']:
            asn = net['asn']
            if net['ix_id'] in mapping_ixp_pop:
                fac_id = mapping_ixp_pop[net['ix_id']]
                lat = self.fac_loc_dict[fac_id]["LATITUDE"]
                lon = self.fac_loc_dict[fac_id]["LONGITUDE"]
                std_lat = self.fac_loc_dict[fac_id]["STD_LATITUDE"]
                std_lon = self.fac_loc_dict[fac_id]["STD_LONGITUDE"]
                std_city = self.fac_loc_dict[fac_id]["STD_CITY"]
                std_state = self.fac_loc_dict[fac_id]["STD_STATE"]
                std_country = self.fac_loc_dict[fac_id]["STD_COUNTRY"]
                ### adding a flag because this is a physical presence
                physical_presence = False
                new_row = [asn, lat, lon, self.data_source, validated,
                           std_lat, std_lon, std_city, std_state, std_country,
                           physical_presence, self.asof_date]
                if lat and lon:
                    if not new_row in self.asn_loc_list:
                        self.asn_loc_list.append(new_row)

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
    input_dir = Path("../unprocessed/PeeringDB")
    output_dir = Path("../processed")
    my_processor = ProcessingPDB(input_dir, output_dir)
    my_processor.run_steps()
