import os
from pathlib import Path
import requests
import json
from time import sleep
from datetime import datetime

class CrawlingEuroIX:
    """
        This class retrieves the IXP and ASN information from the IXP DB run by EuroIX.
    """
    def __init__(self, out_dir):
        self.ixp_url = "https://www.ixpdb.net/explore/dataset/ixp-dataset/download/?"
        self.ixp_url += "format=json&timezone=UTC&lang=en"

        self.asn_url = "https://www.ixpdb.net/explore/dataset/asn-dataset/download/?"
        self.asn_url += "format=json&timezone=UTC&lang=en"

        self.asn_switch_url = "https://www.ixpdb.net/explore/dataset/asn-by-ixp-switch/"
        self.asn_switch_url += "download/?format=json&timezone=UTC&lang=en"

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

        self.ixp_file = "IXPS.json"
        self.asn_file = "ASNS.json"
        self.asn_switch_file = "ASNS-BY-IXP-SWITCH.json"

        if not os.path.isdir(self.out_dir):
            os.makedirs(self.out_dir)

    def run_steps(self):
        print("Retrieving data from EuroIX.")

        if os.path.isfile(self.out_dir / self.ixp_file):
            print(f"\tIXP data already retrieved for {self.date_folder.replace('_', '/')}.")
        else:
            print(f"\tRetrieving: {self.ixp_url}")
            data_page = self.retrieve_page(self.ixp_url)
            data_dict = data_page.json()
            self.save_json(data_dict, self.out_dir / self.ixp_file)

        if os.path.isfile(self.out_dir / self.asn_file):
            print(f"\tASN data already retrieved for {self.date_folder.replace('_', '/')}.")
        else:
            print(f"\tRetrieving: {self.asn_url}")
            data_page = self.retrieve_page(self.asn_url)
            data_dict = data_page.json()
            self.save_json(data_dict, self.out_dir / self.asn_file)

        if os.path.isfile(self.out_dir / self.asn_switch_file):
            print(f"\tASN by IXP switch data already retrieved for ", end='')
            print(f"{self.date_folder.replace('_', '/')}.")
        else:
            print(f"\tRetrieving: {self.asn_switch_url}")
            data_page = self.retrieve_page(self.asn_switch_url)
            data_dict = data_page.json()
            self.save_json(data_dict, self.out_dir / self.asn_switch_file)

    def retrieve_page(self, url):
        page = requests.get(url)
        return page

    def save_json(self, data, f_name):
        print(f"\tSaving to {f_name}.")
        with open(f_name, 'w') as f:
            json.dump(data, f)

if __name__ == "__main__":
    print("This script should not be run by itself. Run it through iGDB.py")
    output_dir = Path("../unprocessed/EuroIX")
    my_crawler = CrawlingEuroIX(output_dir)
    my_crawler.run_steps()
