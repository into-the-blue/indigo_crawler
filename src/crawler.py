from selenium import webdriver
import pandas as pd
from tqdm import tqdm
import time
import json
import codecs
from collections import OrderedDict
import pymongo
import re
import os
from crawlSingleUrl import get_info_of_single_url
from proxy_pool import get_driver_with_proxy
from db import DB
from helper import _print
is_ubuntu = os.environ.get('PY_ENV', 'mac') == 'ubuntu'

db = DB()


class GrapPage(object):
    def __init__(self, city, city_url):
        self.driver = self.initDriver()
        self.city = city
        self.driver_period = 0
        self.city_url = city_url

    def initDriver(self):
        return get_driver_with_proxy()

    def check_driver(self):
        if(self.driver_period >= 10):
            _print('GET NEW PROXY')
            current_url = self.driver.current_url
            self.driver.quit()
            self.driver = self.initDriver()
            self.driver.get(current_url)
            self.driver_period = 0
            return self.driver
        self.driver_period += 1
        return self.driver

    def renew_driver(self):
        current_url = self.driver.current_url
        self.driver.quit()
        self.driver = self.initDriver()
        self.driver.get(current_url)
        return self.driver

    def go_to_next_page(self):
        driver = self.check_driver()
        try:
            next_btn = driver.find_element_by_xpath(
                "//div[@class='content__pg']/div[@class='next']")
            next_btn.click()
        except:
            '''
            next button not found
            '''
            try:
                current_page_elm = driver.find_element_by_xpath(
                    "//div[@class='content__pg']")
                current_page = current_page_elm.get_attribute('data-curpage')
                current_page = int(current_page)
                elm = driver.find_element_by_xpath(
                    "//div[@class='content__pg']/a[@data-page='{}']".format(current_page+1))
                elm.click()
            except:
                _print('BLOCKED, CHANGE PROXY')
                self.renew_driver()

    def read_page_count(self):
        '''
            read page count of url
        '''
        try:
            elm = self.driver.find_element_by_xpath(
                "//div[@class='content__pg']")
            page_count = elm.get_attribute('data-totalpage')
            return int(page_count)
        except:
            return 0

    def get_urls_in_page(self):
        '''
            get urls in one page
        '''
        urls = []
        for i in self.driver.find_elements_by_xpath("//a[@class='content__list--item--aside']"):
            url = i.get_attribute('href')
            if 'apartment' not in url:
                exist = db.save_url(url)
                if exist is False:
                    urls.append(url)
        return urls

    def get_all_urls(self):
        '''
        loop all pages, get all urls
        '''
        page_count = self.read_page_count()
        all_urls = []
        if(page_count != 0):
            all_urls = self.get_urls_in_page()
            for _ in tqdm(range(page_count - 1)):
                time.sleep(2)
                self.go_to_next_page()
                urls = self.get_urls_in_page()
                all_urls += urls
                _print('URLS SAVED!', len(urls), 'TOTAL:', len(all_urls))
        all_urls = list(set(all_urls))
        return all_urls

    def click_order_by_time(self):
        self.driver.find_element_by_link_text('最新上架').click()

    def get_all_station_urls(self, driver):
        stations = driver.find_elements_by_xpath(
            "//div[@class='filter__wrapper w1150']/ul[4]/li/a")
        filterd = filter(lambda x: x.text != '不限', stations)
        filterd = list(map(lambda x: x.get_attribute('href'), filterd))
        return filterd

    def get_all_line_urls(self, driver):
        driver.find_element_by_link_text('按地铁线').click()
        lines = driver.find_elements_by_xpath(
            "//div[@class='filter__wrapper w1150']/ul[3]/li/a")
        filterd = filter(lambda x: x.text != '不限', lines)
        filterd = list(map(lambda x: x.get_attribute('href'), filterd))
        return filterd

    def crawl_data_from_urls(self, urls):
        for url in tqdm(urls):
            driver = self.check_driver()
            driver.get(url)
            _print('START', url)
            try:
                time.sleep(2)
                info = get_info_of_single_url(driver, url)
                if info is None:
                    db.delete_apartment_from_url(url)
                    _print('EXPIRED', url)
                else:
                    db.upsert_apartment(info.get('house_code'), info)
                    _print("SUCCESS", url)
            except:
                _print('ENCOUNTER ERR', url)

    def start_filling_missing_info(self):
        _print('START FILLING')
        all_urls = db.find_missing_apartments()
        _print('CRAWL URL DONE, START CRAWL INFO')
        self.crawl_data_from_urls(all_urls)
        _print('DONE')

    def start(self):
        _print('START')
        self.driver.get(self.city_url)
        all_urls = self.get_all_urls()
        _print('CRAWL URL DONE, START CRAWL INFO')
        self.crawl_data_from_urls(all_urls)
        _print('DONE')

    def start_by_latest(self):
        self.driver.get(self.city_url)
        self.click_order_by_time()
        urls = self.get_all_urls()
        self.crawl_data_from_urls(urls)

    def start_by_lines(self):
        self.driver.get(self.city_url)
        all_line_urls = self.get_all_line_urls(self.driver)
        all_station_urls = []
        for line_url in all_line_urls:
            self.driver.get(line_url)
            all_station_urls += self.get_all_station_urls(self.driver)
        all_station_urls = list(set(all_station_urls))
        _print(len(all_station_urls))
        self.crawl_data_from_urls(all_station_urls)
        # for station_url in tqdm(all_station_urls):
        #     self.get_all_urls(self.driver)

    def quit(self):
        self.driver.quit()


# if __name__ == '__main__':
#     ins = GrapPage('sh', 'https://sh.zu.ke.com/zufang')
#     try:
#         ins.start()
#     finally:
#         pass
#         ins.quit()
