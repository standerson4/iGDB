import os
from pathlib import Path
import requests
import re
import json

class CrawlingPDB:
    def __init__(self, out_dir):
        self.base_url = "https://publicdata.caida.org/datasets/peeringdb-v2/"
        self.out_dir = out_dir
        self.dump_dict = {}
        self.save_file = ""

        if not os.path.isdir(self.out_dir):
            os.makedirs(self.out_dir)

    def run_steps(self):
        print("Retrieving PeeringDB data from CAIDA.")
        self.retrieve_latest_version()
        if self.save_file:
            self.save_latest()

    def retrieve_latest_version(self):
        # we walk through the latest year, then month, then day to find the most recent version

        # first retrieve the index page and get the latest year
        idx_page = self.retrieve_page(self.base_url)
        years_ref = re.findall("<a href=\"20../\">", idx_page.text)
        year = years_ref[-1].replace('<a href="', '').replace('/">', '')

        # next retrieve the year page and get the latest month
        year_url = self.base_url + f"{year}/"
        year_page = self.retrieve_page(year_url)
        months_ref = re.findall("<a href=\"../\">", year_page.text)
        month = months_ref[-1].replace('<a href="', '').replace('/">', '')

        # next retrieve the month page to find the latest json file
        month_url = year_url + f"{month}/"
        month_page = self.retrieve_page(month_url)
        dump_file_list = re.findall("<a href=\"peeringdb_2_dump_20.._.._...json\">", month_page.text)
        dump_file = dump_file_list[-1].replace('<a href="', '').replace('">', '')

        # retrieve the latest file, if it is not already downloaded
        if os.path.isfile(self.out_dir / dump_file):
            print(f"The latest version of PeeringDB ({dump_file}) is already downloaded. ", end='')
            print("Not downloading again.")
            self.save_file = ""
            return
        else:
            self.save_file = dump_file
            dump_url = month_url + f"{dump_file}"
            print(f"Retrieving: {dump_file}. This will take a moment, be patient.")
            raw_dump = self.retrieve_page(dump_url)
            self.dump_dict = raw_dump.json()

    def retrieve_page(self, url):
        page = requests.get(url)
        return page

    def save_latest(self):
        print(f"Saving to {self.out_dir} / {self.save_file}.")
        with open(self.out_dir / self.save_file, 'w') as f:
            json.dump(self.dump_dict, f, indent=4)

if __name__ == "__main__":
    print("This script should not be run by itself. Run it through iGDB.py")
    output_dir = Path("../unprocessed/PeeringDB")
    my_crawler = CrawlingPDB(output_dir)
    my_crawler.run_steps()
