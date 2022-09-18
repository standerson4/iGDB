import os
from pathlib import Path
import requests
import json
from time import sleep
from datetime import datetime

class CrawlingASRank:
    """
        This class retrieves all the ASNs, ASN links, and organizations
        from CAIDA ASRank.
        It first pulls json files with subsets of the required information.
        Then it combines all the individual files into a single file.
    """
    def __init__(self, out_dir):
        self.asn_base_url = "https://api.asrank.caida.org/v2/restful/asns/?offset=XXX&first=YYY"
        self.links_base_url = "https://api.asrank.caida.org/v2/restful/asnLinks/?offset=XXX&first=YYY"
        self.orgs_base_url = "https://api.asrank.caida.org/v2/restful/organizations/?offset=XXX&first=YYY"
        self.first = 10000
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
        self.asn_file = "ASNS-offsetXXX.json"
        self.links_file = "LINKS-offsetXXX.json"
        self.orgs_file = "ORGS-offsetXXX.json"

        if not os.path.isdir(self.out_dir):
            os.makedirs(self.out_dir)

    def run_steps(self):
        print("Retrieving data from ASRank.")
        # if the most recent data already exists, don't download any new data
        if os.path.isfile(self.out_dir / self.asn_file.replace("-offsetXXX",'')):
            if os.path.isfile(self.out_dir / self.links_file.replace("-offsetXXX", '')):
                if os.path.isfile(self.out_dir / self.orgs_file.replace("-offsetXXX", '')):
                    print(f"\tData already retrieved for {self.date_folder.replace('_', '/')}.")
                    return

        # retrieve all the ASN files
        self.retrieve_asn_files()

        # retrieve all the ASN links files
        self.retrieve_links_files()

        # retrieve all the organizations files
        self.retrieve_orgs_files()

        # combine all the individual files by offset into a single file
        #  and remove all the individual offset files
        self.combine_files('ASNS')
        self.combine_files('LINKS')
        self.combine_files('ORGS')

    def retrieve_asn_files(self):
        count = 0
        while True:
            save_file = self.asn_file.replace("XXX", str(count))
            if os.path.isfile(self.out_dir / save_file):
                print(f"\t{save_file} already exists. Continuing.")
                count += self.first
                continue
            url = self.asn_base_url
            url = url.replace("XXX", str(count))
            url = url.replace("YYY", str(self.first))
            print(f"\tRetrieving: {url}")
            asn_page = self.retrieve_page(url)
            asn_dict = asn_page.json()
            try:
                save_dict = asn_dict["data"]["asns"]["edges"]
            except:
                print(f"\tCould not parse data for url: {url}")
                continue
            self.save_json(save_dict, self.out_dir / save_file)
            count += self.first
            is_next = asn_dict["data"]["asns"]["pageInfo"]["hasNextPage"]
            sleep(1) # to ensure we don't put too much load on the server
            if not is_next:
                break

    def retrieve_links_files(self):
        count = 0
        while True:
            save_file = self.links_file.replace("XXX", str(count))
            if os.path.isfile(self.out_dir / save_file):
                print(f"\t{save_file} already exists. Continuing.")
                count += self.first
                continue
            url = self.links_base_url
            url = url.replace("XXX", str(count))
            url = url.replace("YYY", str(self.first))
            print(f"\tRetrieving: {url}")
            links_page = self.retrieve_page(url)
            links_dict = links_page.json()
            try:
                save_dict = links_dict["data"]["asnLinks"]["edges"]
            except:
                print(f"\tCould not parse data for url: {url}")
                continue
            self.save_json(save_dict, self.out_dir / save_file)
            count += self.first
            is_next = links_dict["data"]["asnLinks"]["pageInfo"]["hasNextPage"]
            sleep(1) # to ensure we don't put too much load on the server
            if not is_next:
                break

    def retrieve_orgs_files(self):
        count = 0
        while True:
            save_file = self.orgs_file.replace("XXX", str(count))
            if os.path.isfile(self.out_dir / save_file):
                print(f"\t{save_file} already exists. Continuing.")
                count += self.first
                continue
            url = self.orgs_base_url
            url = url.replace("XXX", str(count))
            url = url.replace("YYY", str(self.first))
            print(f"\tRetrieving: {url}")
            orgs_page = self.retrieve_page(url)
            orgs_dict = orgs_page.json()
            try:
                save_dict = orgs_dict["data"]["organizations"]["edges"]
            except:
                print(f"\tCould not parse data for url: {url}")
                continue
            self.save_json(save_dict, self.out_dir / save_file)
            count += self.first
            is_next = orgs_dict["data"]["organizations"]["pageInfo"]["hasNextPage"]
            sleep(1) # to ensure we don't put too much load on the server
            if not is_next:
                break

    def retrieve_page(self, url):
        page = requests.get(url)
        return page

    def combine_files(self, f_type):
        print("\tCombining individual files into a single json file.")
        data_list = []
        for f in os.listdir(self.out_dir):
            if not f_type in f:
                continue
            if not 'offset' in f:
                continue
            with open(self.out_dir / f, 'r') as f:
                sub_list = json.load(f)
                for e in sub_list:
                    data_list.append(e)
        if data_list:
            all_file = f"{f_type}.json"
            self.save_json(data_list, self.out_dir / all_file)
        else:
            print("No data to save.")

        # remove all the individual offset files
        for f in os.listdir(self.out_dir):
            if not f_type in f:
                continue
            if not 'offset' in f:
                continue
            os.remove(self.out_dir / f)

    def save_json(self, data, f_name):
        print(f"Saving to {f_name}.")
        with open(f_name, 'w') as f:
            json.dump(data, f)

if __name__ == "__main__":
    print("This script should not be run by itself. Run it through iGDB.py")
    output_dir = Path("../unprocessed/ASRank")
    my_crawler = CrawlingASRank(output_dir)
    my_crawler.run_steps()
