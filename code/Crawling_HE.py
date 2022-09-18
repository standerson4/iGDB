import os, time, json

from concurrent.futures import ThreadPoolExecutor, as_completed

from pathlib import Path

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By


class CrawlingHE:
    _SLEEP_DURATION = 5*60
    _MAX_RETRIES = 10
    def __init__(self, out_dir):
        self._ixp_summary_url = "http://bgp.he.net/report/exchanges"
        self._ixp_base_url = "http://bgp.he.net/exchange"
        self._out_dir = out_dir        

    def run_steps(self):
        print("Retrieving IXP data from Hurricane Electric")
        self._ixps = self._fetch_ixp_list()
        self._fill_ixps()
        self.save_json(self._out_dir / ("he_dump_"+time.strftime("%Y%m%d")+".json"))

    def _fetch_ixp_list(self):
        options = FirefoxOptions()
        options.add_argument("--headless")
        browser = webdriver.Firefox(options=options)
        profile = webdriver.FirefoxProfile()
        profile.set_preference("general.useragent.override", 
            "Mozilla/5.0 (Windows NT 6.3; WOW64; rv:44.0) Gecko/20100101 Firefox/44.0")
        
        browser.get(self._ixp_summary_url)
        ixps = {}
        ixp_table = browser.find_element_by_xpath("id('exchangestable')/tbody")
        ixp_rows = ixp_table.find_elements_by_tag_name("tr")
        for row in ixp_rows:
            tds = row.find_elements_by_tag_name("td")
            if tds:
                a_element = tds[0].find_element_by_tag_name("a")
                name = a_element.text
                link = a_element.get_attribute("href")
                members = int(tds[1].text.replace(",",""))
                data  = tds[2].find_element_by_tag_name("img").get_attribute('alt')
                cc = tds[3].text
                city = tds[4].text
                website = tds[5].find_element_by_tag_name("a").get_attribute("href")

                ixps[name] = {
                              "name" : name,
                              "members" : members,
                              "data_feed_health" : data,
                              "cc" : cc,
                              "city" : city,
                              "ixp_external_url" : website,
                              "he_url" : link
                             }
        browser.quit()
        print(f"Found {len(ixps)} IXPs HE summary list")
        return ixps

    def _fill_ixps(self, num_workers=4):
        pool = ThreadPoolExecutor(max_workers=num_workers)
        submitted_futures = {}
        for ixp in self._ixps:
            ft = pool.submit(self._fill_ixp, ixp)
            submitted_futures[ft] = ixp
        # we wait fot the enqueued futures to complete, collect the results.
        while submitted_futures:
            # we could pass in an overall timeout based on the # worker threads here, or even timeout
            # for individual future from the time they started running.
            for ft in as_completed(submitted_futures.keys()):
                item = submitted_futures[ft]
                del submitted_futures[ft]
                try:
                    res = ft.result()
                except Exception as e:
                    if isinstance(e, KeyboardInterrupt):
                        raise e
                    print("Exception while running work item %s: %s", item, e)

    def _fill_ixp(self, ixp):
        url = self._ixps[ixp]["he_url"]
        # Due to the fact that selenium webdriver is not thread safe, we launch an instance of webdriver per
        # thread. See: https://github.com/SeleniumHQ/selenium/wiki/Frequently-Asked-Questions#q-is-webdriver-thread-safe
        # We don't do anything crazy and just launch the browser sessions in "headless" mode.
        options = FirefoxOptions()
        options.add_argument("--headless")
        browser = webdriver.Firefox(options=options)
        profile = webdriver.FirefoxProfile()
        profile.set_preference("general.useragent.override",
                               "Mozilla/5.0 (Windows NT 6.3; WOW64; rv:44.0) Gecko/20100101 Firefox/44.0")
        attempt = 1
        while attempt <= CrawlingHE._MAX_RETRIES:
            try:
                browser.find_element_by_tag_name('body').send_keys(Keys.COMMAND + 't')
                browser.get(url)
                elem = WebDriverWait(browser, 30).until(
                    EC.presence_of_element_located((By.ID, "exchange")))
                property_names = browser.find_elements_by_class_name("asleft")
                property_values = browser.find_elements_by_class_name("asright")
                #self._ixps[ixp]["etc"] = {}
                for name, value in zip(property_names, property_values):
                    if "IPv4 Prefixes:" in name.text:
                        self._ixps[ixp]["v4_pfxs"] = value.text.split(", ")
                    elif "IPv6 Prefixes:" in name.text:
                        self._ixps[ixp]["v6_pfxs"] = value.text.split(", ")
                    else:
                        key = name.text.strip().replace(":","").lower().replace(" ", "_")
                        if key in self._ixps[ixp]: 
                            continue
                        elif key == "data_feed_health":
                            self._ixps[ixp][key] = value.find_element_by_tag_name("img").get_attribute('alt')
                        else:
                            self._ixps[ixp][key] = value.text

                self._ixps[ixp]["members"] = []
                members_table = browser.find_elements_by_id("members")
                if not members_table:
                    browser.find_element_by_tag_name('body').send_keys(Keys.COMMAND + 'w')
                    break
                trs = members_table[0].find_elements_by_tag_name("tr")
                for row in trs:
                    tds = row.find_elements_by_tag_name("td")
                    if len(tds) > 0:
                        a_element = tds[0].find_element_by_tag_name("a")
                        asn = a_element.text
                        he_asn_url = a_element.get_attribute("href")
                        self._ixps[ixp]['members'].append({
                                                "asn" : asn,
                                                "as_name" : tds[1].text,
                                                "he_asn_url" : he_asn_url,
                                                "v4_ips" : tds[2].text.split("\n"),
                                                "v6_ips" : tds[3].text.split("\n")
                                              })
                browser.find_element_by_tag_name('body').send_keys(Keys.COMMAND + 'w')
                break
            except Exception as e:
                print(ixp, type(e).__name__, e)
                attempt += 1
                time.sleep(CrawlingHE._SLEEP_DURATION)
        browser.quit()

    def save_json(self, f_name):
        print(f"Saving to {f_name}.")
        with open(f_name, 'w') as f:
            json.dump(list(self._ixps.values()), f, indent=4)

if __name__ == "__main__":
    print("This script should not be run by itself. Run it through iGDB.py")
    output_dir = Path("../unprocessed/HE")
    my_crawler = CrawlingHE(output_dir)
    my_crawler.run_steps()
