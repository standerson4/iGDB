import os
from pathlib import Path
import requests
import re
import json
from time import sleep
from datetime import datetime

class CrawlingPCH:
    """
        This class retrieves all the active IXPs and the subnets used at each IXP
        from Packet Clearinghouse.
        It first pulls a json file with the active IXPs.
        Then pulls the subnets used at each active IXP into an individual file.
        Then it combines all the individual files into a single file.
    """
    def __init__(self, out_dir):
        self.ixp_base_url = "https://www.pch.net/api/ixp/directory/Active"
        self.subnets_base_url = "https://www.pch.net/api/ixp/subnet_details/"
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
        self.active_idx = {}
        self.active_file = "pch_active_ixp.json"
        self.subnets_file = "pch_subnets_XX.json"

        if not os.path.isdir(self.out_dir):
            os.makedirs(self.out_dir)

    def run_steps(self):
        print("Retrieving data from Packet Clearinghouse.")
        # if the most recent data already exists, don't download any new data
        if os.path.isfile(self.out_dir / self.active_file):
            if os.path.isfile(self.out_dir / self.subnets_file.replace("_XX", '')):
                print(f"Data already retrieved for {self.date_folder.replace('_', '/')}.")
                return

        # first retrieve the index page and get the IXP indexes
        self.retrieve_and_save_active_idx()
        # next retrieve the subnets page for each IXP and save locally
        self.retrieve_and_save_subnets()
        # combine all the individual subnets files into a single file
        #  and remove all the individual subnets files
        self.combine_subnets_files()

    def retrieve_and_save_active_idx(self):
        active_idx_page = self.retrieve_page(self.ixp_base_url)
        self.active_idx = active_idx_page.json()
        self.save_json(self.active_idx, self.out_dir / self.active_file)
        sleep(0.5) # to ensure we don't put too much load on the server

    def retrieve_and_save_subnets(self):
        for ixp_dict in self.active_idx:
            ixp_id = ixp_dict['id']
            save_file = self.subnets_file.replace('XX', str(ixp_id))
            if os.path.isfile(self.out_dir / save_file):
                continue
            subnets_url = self.subnets_base_url + str(ixp_id)
            print(f"Retrieving {subnets_url}")
            page = self.retrieve_page(subnets_url)
            subnets_dict = page.json()
            self.save_json(subnets_dict, self.out_dir / save_file)
            sleep(0.5) # to ensure we don't put too much load on the server

    def retrieve_page(self, url):
        page = requests.get(url)
        return page

    def combine_subnets_files(self):
        print("Combining individual files into a single json file.")
        all_subnets = {}
        for ixp_dict in self.active_idx:
            ixp_id = ixp_dict['id']
            in_file = self.subnets_file.replace('XX', str(ixp_id))

            if os.path.isfile(self.out_dir / in_file):
                with open(self.out_dir / in_file, 'r') as f:
                    subnets_dict = json.load(f)
                all_subnets[ixp_id] = subnets_dict
                os.remove(self.out_dir / in_file)
            else:
                print(f"{in_file} does not exist")
        all_file = self.subnets_file.replace('_XX', '')
        self.save_json(all_subnets, self.out_dir / all_file)

    def save_json(self, data, f_name):
        print(f"Saving to {f_name}.")
        with open(f_name, 'w') as f:
            json.dump(data, f, indent=4)

if __name__ == "__main__":
    print("This script should not be run by itself. Run it through iGDB.py")
    output_dir = Path("../unprocessed/PCH")
    my_crawler = CrawlingPCH(output_dir)
    my_crawler.run_steps()
