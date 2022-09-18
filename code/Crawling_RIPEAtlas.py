import os
from pathlib import Path
import requests
import json
from time import sleep
from datetime import datetime

class CrawlingRIPEAtlas:
    def __init__(self, out_dir, replace_existing):
        self.anchors_url = "https://atlas.ripe.net/api/v2/anchors/?format=json&page=XX"
        self.probes_url = "https://atlas.ripe.net/api/v2/probes/?format=json&page=XX"
        self.replace_existing = replace_existing

        today = datetime.now()
        year = today.year
        if today.month < 10:
            month = f"0{str(today.month)}"
        else:
            month = today.month
        if today.day < 10:
            day = f"0{str(today.day)}"
        else:
            day = today.day
        self.date_folder = f"{year}_{month}_{day}"
        self.out_dir = out_dir / self.date_folder

        if not os.path.isdir(self.out_dir):
            os.makedirs(self.out_dir)

    def run_steps(self):
        print("Retrieving the RIPE Atlas anchors.")
        self.retrieve_anchors()

        print("Retrieving the RIPE Atlas probes.")
        self.retrieve_probes()

    def retrieve_anchors(self):
        p_num = 1
        while True:
            save_fname = f"anchors_{p_num}.json"
            url = self.anchors_url.replace('XX', str(p_num))

            # if the local file exists and we don't want to replace it, continue
            if self.replace_existing == 'N':
                if os.path.isfile(self.out_dir / save_fname):
                    print(f"Skipping {save_fname}")
                    p_num += 1
                    continue

            print(f"Retrieving {url}")
            data = requests.get(url)
            data_j = data.json()
            if 'error' in data_j.keys():
                print(data_j)
                break
            self.save_file(data_j, self.out_dir / save_fname)
            if 'next' in data_j.keys() and not data_j['next']:
                    break

            p_num += 1
            sleep(0.5)
            # input("ENTER to continue.")

    def retrieve_probes(self):
        p_num = 1
        while True:
            save_fname = f"probes_{p_num}.json"
            url = self.probes_url.replace('XX', str(p_num))

            # if the local file exists and we don't want to replace it, continue
            if self.replace_existing == 'N':
                if os.path.isfile(self.out_dir / save_fname):
                    print(f"Skipping {save_fname}")
                    p_num += 1
                    continue

            print(f"Retrieving {url}")
            data = requests.get(url)
            data_j = data.json()
            if 'error' in data_j.keys():
                print(data_j)
                break
            self.save_file(data_j, self.out_dir / save_fname)
            if 'next' in data_j.keys() and not data_j['next']:
                    break

            p_num += 1
            sleep(0.5)
            # input("ENTER to continue")

    def save_file(self, data, f_name):
        with open(f_name, 'w') as f:
            json.dump(data, f)

if __name__ == "__main__":
    print("This script should not be run by itself. Run it through iGDB.py")
    output_dir = Path("../unprocessed/RIPEAtlas")
    my_retriever = CrawlingRIPEAtlas(output_dir, 'N')
    my_retriever.run_steps()

