import os
from pathlib import Path
import requests
import json
import csv
from time import sleep
from datetime import datetime
from datetime import timedelta
from ripe.atlas.cousteau import AtlasResultsRequest

class CrawlingRIPETraceroutes:
    def __init__(self, out_dir, ripe_dir):
        self.ripe_dir = ripe_dir
        self.msm_url = "https://atlas.ripe.net/api/v2/measurements/?"
        self.msm_url += "type=traceroute&target=XX&is_public=true&status=2&af=4"
        self.msm_url += "&description__contains=%22Anchoring%20Mesh%20Measurement%22"

        today = datetime.now()
        yesterday = today - timedelta(days=1)
        year = yesterday.year
        if yesterday.month < 10:
            month = f"0{str(yesterday.month)}"
        else:
            month = yesterday.month
        if yesterday.day < 10:
            day = f"0{str(yesterday.day)}"
        else:
            day = yesterday.day
        self.date_folder = f"{year}_{month}_{day}"
        self.out_dir = out_dir / self.date_folder

        if not os.path.isdir(self.out_dir):
            os.makedirs(self.out_dir)

        self.anchors_list = []
        self.msm_id_list = []
        self.msm_id_header = ["ANCHOR_ID", "TRACEROUTE_MEASUREMENT_ID"]
        self.msm_id_file = self.out_dir / "anchor_traceroute_measurement_id.csv"

        self.start_date = (yesterday.year, yesterday.month, yesterday.day, 12, 0)
        self.end_date = (yesterday.year, yesterday.month, yesterday.day, 12, 30)
        self.traceroute_results = {}
        self.traceroute_file = "anchor_traceroute_results_XXX.json"

    def run_steps(self):
        # open the anchors files
        if not os.path.isdir(self.ripe_dir):
            print(f"No existing RIPE anchor data exists. ", end='')
            print(f"Please update RIPE anchor data with: python iGDB.py -u ripe", end='')
            return
        self.read_anchors_file()
        # retrieve the measurement ID for traceroute
        if not os.path.isfile(self.msm_id_file):
            self.retrieve_msm_id()
            self.save_csv(self.msm_id_list, self.msm_id_header, self.msm_id_file)
        else:
            print(f"\tTraceroute measurement ID file exists, reading existing file.")
            self.msm_id_list = self.read_csv(self.msm_id_file)
        # retrieve the corresponding traceroutes for one hour that happened yesterday
        if not os.path.isfile(self.traceroute_file):
            self.retrieve_traceroutes()
        else:
            print(f"\tTraceroute measurement results file exists for {self.date_folder.replace('_', '/')}.")

    def read_anchors_file(self):
        for dd in os.listdir(self.ripe_dir):
            for af in os.listdir(self.ripe_dir / dd):
                if not 'anchors' in af:
                    continue
                with open(self.ripe_dir / dd / af) as f:
                    temp_dict = json.load(f)
                for r in temp_dict['results']:
                    pid = r['probe']
                    fqdn = r['fqdn']
                    is_anchor = r['type']
                    is_disabled = r['is_disabled']
                    if is_anchor == 'Anchor' and not is_disabled:
                        self.anchors_list.append((pid, fqdn))

    def retrieve_msm_id(self):
        print("\tRetrieving traceroute IPv4 measurement IDs.")
        count = 0 # for development
        for row in self.anchors_list:
            p_id = row[0]
            fqdn = row[1]

            url = self.msm_url.replace('XX', fqdn)

            print(f"\n\tRetrieving {url}")
            data = requests.get(url, timeout=10)
            data_j = data.json()
            if 'error' in data_j.keys():
                print(data_j)
                continue
            try:
                msm_id = data_j['results'][0]['id']
            except:
                print(f"\tNo traceroute IPv4 measurement for anchor {p_id}.")
                continue
            self.msm_id_list.append((p_id, msm_id))

            # for development
            count += 1
            if count >= 50:
                break

            sleep(0.5)

    def retrieve_traceroutes(self):
        all_pids = []
        for row in self.msm_id_list:
            all_pids.append(row[0])

        for row in self.msm_id_list:
            t_pid = row[0]
            msm_id = row[1]
            save_file = self.traceroute_file.replace("XXX", str(t_pid))
            if os.path.isfile(self.out_dir / save_file):
                print(f"Traceroute IPv4 data already downloaded for {t_pid}. Continuing.")
                continue
            print(f"\tRetrieving traceroute IPv4 measurement results for {t_pid}.")
            self.retrieve_msm(t_pid, msm_id, all_pids)
            self.save_json(self.traceroute_results[t_pid], self.out_dir / save_file)

    def retrieve_msm(self, target_pid, msm, source_pids):
        s_year = self.start_date[0]
        s_month = self.start_date[1]
        s_day = self.start_date[2]
        s_hour = self.start_date[3]
        s_min = self.start_date[4]

        e_year = self.end_date[0]
        e_month = self.end_date[1]
        e_day = self.end_date[2]
        e_hour = self.end_date[3]
        e_min = self.end_date[4]

        kwargs = {
                "msm_id": msm,
                "start": datetime(s_year, s_month, s_day, s_hour, s_min),
                "stop": datetime(e_year, e_month, e_day, e_hour, e_min),
                "probe_ids": source_pids
        }

        is_success, self.traceroute_results[target_pid] = AtlasResultsRequest(**kwargs).create()
        sleep(0.5)
        if not is_success:
            print("\tRetrieval unsuccessful")

    def read_csv(self, f_name):
        print(f"\tReading from {f_name}.")
        results = []
        with open(f_name, 'r') as f:
            csv_reader = csv.reader(f, delimiter=',')
            header = next(csv_reader)
            for row in csv_reader:
                results.append(row)
        return results

    def save_json(self, data, f_name):
        print(f"\tSaving to {f_name}.")
        with open(f_name, 'w') as f:
            json.dump(data, f)

    def save_csv(self, data, header, f_name):
        print(f"\tSaving to {f_name}.")
        with open(f_name, 'w') as f:
            csv_writer = csv.writer(f, delimiter=',')
            csv_writer.writerow(header)
            for d in data:
                csv_writer.writerow(d)


if __name__ == "__main__":
    print("This script should not be run by itself. Run it through iGDB.py")
    output_dir = Path("../unprocessed/RIPETraceroutes")
    ripe_dir = Path("../unprocessed/RIPEAtlas")
    my_retriever = CrawlingRIPETraceroutes(output_dir, ripe_dir)
    my_retriever.run_steps()

