import os
from pathlib import Path
import requests
import json
from datetime import date
from time import sleep

class CrawlingTelegeography:
    def __init__(self, out_dir):
        self.base_url = "https://raw.githubusercontent.com/telegeography/www.submarinecablemap.com/master/web/public/api/v3/"
        self.map_url = self.base_url + "cable/cable-geo.json"
        self.landing_url = self.base_url + "landing-point/landing-point-geo.json"
        self.out_dir = out_dir
        t = str(date.today()).replace('-', '_')
        self.cable_save_file = f"cable-geo_{t}.json"
        self.landing_save_file = f"landing-point-geo_{t}.json"

        self.cable_data_dir = out_dir / 'cable_data'

        if not os.path.isdir(self.out_dir):
            os.makedirs(self.out_dir)

        if not os.path.isdir(self.cable_data_dir):
            os.makedirs(self.cable_data_dir)

    def run_steps(self):
        print("Retrieving submarine cable data from Telegeography.")
        # retrieve the submarine cable geojson file
        if os.path.isfile(self.out_dir / self.cable_save_file):
            print(f"\tThe latest version of cable map ({self.cable_save_file}) ", end='')
            print(f"is already downloaded. ", end='')
            print("Not downloading again.")
        else:
            print(f"\tRetrieving latest cable map.")
            raw_dump = self.retrieve_page(self.map_url)
            dump_dict = raw_dump.json()
            self.save_results(dump_dict, self.out_dir / self.cable_save_file)

        # retrieve the individual submarine cable info files
        self.retrieve_cable_info()

        # retrieve the landing point geojson file
        if os.path.isfile(self.out_dir / self.landing_save_file):
            print(f"\nThe latest version of landing points ({self.landing_save_file}) ", end='')
            print(f"is already downloaded. ", end='')
            print("Not downloading again.")
        else:
            print(f"\nRetrieving latest landing point map.")
            raw_dump = self.retrieve_page(self.landing_url)
            dump_dict = raw_dump.json()
            self.save_results(dump_dict, self.out_dir / self.landing_save_file)

    def retrieve_cable_info(self):
        cable_file = self.out_dir / self.cable_save_file
        with open(cable_file, 'r') as f:
            cable_dict = json.load(f)

        t = str(date.today()).replace('-', '_')

        for c in cable_dict['features']:
            c_id = c['properties']['id']
            save_file = f"{c_id}_{t}.json"
            if os.path.isfile(self.cable_data_dir / save_file):
                print(f"File {save_file} already exists. Skipping it.")
                continue
            print(f"Retrieving {c_id} cable info.")
            url = self.base_url + f'cable/{c_id}.json'
            raw_dump = self.retrieve_page(url)
            dump_dict = raw_dump.json()
            self.save_results(dump_dict, self.cable_data_dir / save_file)
            sleep(0.5)

    def retrieve_page(self, url):
        page = requests.get(url)
        return page

    def save_results(self, data, f_name):
        print(f"Saving to {f_name}.")
        with open(f_name, 'w') as f:
            json.dump(data, f, indent=4)

if __name__ == "__main__":
    print("This script should not be run by itself. Run it through iGDB.py")
    output_dir = Path("../unprocessed/Telegeography")
    my_crawler = CrawlingTelegeography(output_dir)
    my_crawler.run_steps()
