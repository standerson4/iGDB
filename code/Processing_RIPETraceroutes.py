from pathlib import Path
import os
import json
import csv
from datetime import date
from datetime import timedelta
import dbStructure

class ProcessingRIPETraceroutes:
    def __init__(self, in_dir, out_dir):
        self.in_dir = in_dir
        self.asof_date = ''
        if not os.path.isdir(in_dir):
            print(f"{in_dir} does not exist.")
            return
        self.out_dir =  out_dir
        self.traceroutes_list = []
        name, fields = self.read_fields(dbStructure.sql_create_traceroutes_table)
        self.traceroutes_header = fields
        self.traceroutes_table = name 

        self.data_source = "RIPEAtlas"

        if not os.path.isdir(self.out_dir / self.traceroutes_table):
            os.makedirs(self.out_dir / self.traceroutes_table)

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
        print("Processing local RIPE Atlas traceroute data. ", end='')
        if not os.path.isdir(self.in_dir):
            print("\n\tThere is no data to process. Update the RIPE Atlas traceroute data.")
            return
        print("This takes a while. Status reported every 25 files.")
        for d in os.listdir(self.in_dir):
            year = d.split('_')[0]
            month = d.split('_')[1]
            day = d.split('_')[2]
            self.asof_date = f"{year}-{month}-{day}"

            for i, f in enumerate(os.listdir(self.in_dir / d)):
                if (i+1) % 25 == 0:
                    print(f"\tWorking on file {i+1} of {len(os.listdir(self.in_dir / d))}.")
                if 'measurement_id' in f:
                    continue
                elif 'traceroute_results' in f:
                    self.read_traceroute_file(self.in_dir / d / f)

        trace_file = self.out_dir / self.traceroutes_table / f"{self.data_source}_{self.traceroutes_table}.csv"
        self.save_csv(self.traceroutes_list, self.traceroutes_header, trace_file)

    def read_traceroute_file(self, f_name):
        #print(f"Reading: {f_name}")
        with open(f_name, 'r') as f:
            tr_dict = json.load(f)
        for m in tr_dict:
            try:
                src_ip = m['src_addr']
                dst_ip = m['dst_addr']
                timestamp = m['timestamp']
            except:
                continue
            for h in m['result']:
                try:
                    hop = h['hop']
                except:
                    continue
                for r in h['result']:
                    try:
                        hop_ip = r['from']
                        ttl = r['ttl']
                        rtt = r['rtt']
                    except:
                        continue
                    new_row = [src_ip, dst_ip, hop_ip, ttl, rtt,
                            self.data_source, timestamp, self.asof_date]
                    self.traceroutes_list.append(new_row)

    def save_csv(self, data, header, f_name):
        print(f"\tSaving to {f_name}.")
        with open(f_name, 'w') as f:
            csv_writer = csv.writer(f, delimiter=',')
            csv_writer.writerow(header)
            for d in data:
                csv_writer.writerow(d)

if __name__ == "__main__":
    print("This script should not be run by itself. Run it through iGDB.py")
    input_dir = Path("../unprocessed/RIPETraceroutes")
    output_dir = Path("../processed")
    my_processor = ProcessingRIPETraceroutes(input_dir, output_dir)
    my_processor.run_steps()

