from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from bs4 import BeautifulSoup
from tldextract import extract
from time import strftime, gmtime
from socket import gethostbyname
from queue import Queue
import psycopg2
from pyvirtualdisplay import Display
import json


class RevContentScraper:
    def __init__(self, proxy_address: str, user_agent: str = None, width: int = 1920, height: int = 1080,
                 whole_internet: bool = False):
        self.whole_internet = whole_internet
        self.start_points = []
        self.proxy = proxy_address
        dc = DesiredCapabilities.PHANTOMJS
        if user_agent is not None:
            dc["phantomjs.page.settings.userAgent"] = user_agent
        self.d = Display(size=(width, height), visible=0)
        self.d.start()
        self.driver = webdriver.PhantomJS(
            desired_capabilities=dc, service_args=['--proxy=%s' % proxy_address]) # TODO enable proxies
        self.driver.set_window_size(width=width, height=height)

    def add_start_points_from_file(self, filename: str) -> None:
        with open(filename) as FP:
            self.start_points = [line.strip() for line in FP.readlines()]

    def add_start_point_from_command_line(self, start_point: str):
        self.start_points = [start_point]

    def extract_urls_to_follow(self) -> list:
        code = self.driver.page_source
        links = set()
        for elem in BeautifulSoup(code, "lxml").find_all("a"):
            try:
                links.add(elem['href'])
            except KeyError:
                pass
        try:
            links.remove(self.driver.current_url)
        except KeyError:
            pass
        if not self.whole_internet:
            current_domain = extract(self.driver.current_url).domain
            for link in links.copy():
                if extract(link).domain != current_domain:
                    try:
                        links.remove(link)
                    except KeyError:
                        pass
        return list(links)

    def scrape(self):
        q = Queue()
        for start_point in self.start_points:
            q.put(start_point)
        while not q.empty():
            self.driver.get(q.get())
            for p in self.extract_urls_to_follow():
                q.put(p)
            self.process_page()

    def process_page(self):
        widgets = BeautifulSoup(self.driver.page_source, "lxml").find_all("div", {"class": "rc-wc"})
        ads = []
        print(len(widgets))
        for widget in widgets:
            ads_sources = widget.find_all("a", {"class": "rc-cta"})
            for ad_source in ads_sources:
                redirects = ["http:" + ad_source['href']]
                ad = {
                    "site": extract(self.driver.current_url).domain + "." + extract(self.driver.current_url).suffix,
                    "proxy": self.proxy,
                    "page_url": self.driver.current_url,
                    "rank": ads_sources.index(ad_source) + 1,  # no. of ad in the widget
                    "headline": ad_source['title'],  # ad headline
                    "brand": " ",
                    "widget": widget['data-id'],  # widget id
                    "content": ad_source['data-id'],  # Content ID for this particular ad
                    "network": "RevContent",  # ads network
                    "image": ad_source.find("div", {"class": "rc-photo"})['style'].split('url(')[1].split(');')[
                        0],
                    "device_type": " ",
                    "os_type": " ",
                    "created_at": strftime("%a, %d %b %Y %H:%M:%S +0000", gmtime())
                }
                if ad_source.find("div", {"class": "rc-provider"}) is not None:
                    ad["brand"] = ad_source.find("div", {"class": "rc-provider"}).text
                self.driver.get("http:" + ad_source['href'])
                ad["redirects"] = json.dumps(["http:" + ad_source['href'], self.driver.current_url])
                ad.update({
                    "lander_url": self.driver.current_url,  # url of page after clicking ad
                    "lander_screenshot": self.driver.get_screenshot_as_base64(),
                    "lander_ip": gethostbyname(
                        extract(self.driver.current_url).domain + "." + extract(self.driver.current_url).suffix),
                    "outbound_links": json.dumps(self.extract_urls_to_follow())
                    # JSON encoded list of all the outbound links off the lander, with redirects
                })
                ads.append(ad)
                print("Trying to save result to db")
                self.save_result_to_db(ad)

    @staticmethod
    def save_result_to_db(ad: dict):
        try:
            conn = psycopg2.connect("dbname='revcontent' user='dbuser' host='localhost' password='12345'")
            cur = conn.cursor()
            insert_statement = 'INSERT INTO "ads"  (%S) VALUES %S'
            cur.execute(insert_statement, (psycopg2.extensions.AsIs(','.join(list(ad.keys()))), tuple(ad.values())))
            conn.commit()
        except:
            print("Database writing failed")
            quit()

    def __del__(self):
        self.driver.close()
        self.d.stop()


a = RevContentScraper("218.76.106.78:3128",
                      user_agent="Mozilla/5.0 (Windows NT 6.2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36")
a.add_start_points_from_file("startpoints.txt")
a.scrape()
