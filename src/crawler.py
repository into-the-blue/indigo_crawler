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
from proxy_pool import get_driver_with_proxy, delete_proxy
from baiduMap.getCoordinates import getGeoInfo
from db.db import db
from utils.util import _print, _error
is_ubuntu = os.getenv('PY_ENV', 'mac') == 'ubuntu'


class GrapPage(object):
    def __init__(self, city, city_url):
        self.driver = self.initDriver()
        self.city = city
        self.driver_period = 0
        self.city_url = city_url

    def initDriver(self):
        return get_driver_with_proxy()

    def check_driver(self, open_last_page=True,  force=False):
        if(self.driver_period >= 10 or force):
            _print('GET NEW PROXY')
            current_url = self.driver.current_url
            self.driver.quit()
            self.driver = self.initDriver()
            if open_last_page:
                self.driver.get(current_url)
            self.driver_period = 0
            return self.driver
        self.driver_period += 1
        return self.driver

    def _get(self, url, times=0):
        if(times > 5):
            raise Exception('TOO MANY TIMES')
        try:
            self.driver.set_page_load_timeout(10)
            self.driver.get(url)
            return self.driver
        except Exception as e:
            _error('PROXY BLOCKED', e)
            delete_proxy()
            self.check_driver(force=True, open_last_page=False)
            return self._get(url, times=times + 1)

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
                self.check_driver(force=True)

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

    def get_urls_in_page(self, station_info=None):
        '''
            get urls in one page
        '''
        urls = []
        for i in self.driver.find_elements_by_xpath("//a[@class='content__list--item--aside']"):
            url = i.get_attribute('href')
            if 'apartment' not in url:
                exist = db.save_url(url, station_info)
                if exist is False:
                    urls.append(url)
        return urls

    def get_all_urls(self, station_info=None):
        '''
        loop all pages, get all urls
        '''
        page_count = self.read_page_count()
        all_urls = []
        if(page_count != 0):
            all_urls = self.get_urls_in_page(station_info)
            for _ in tqdm(range(page_count - 1)):
                time.sleep(2)
                self.go_to_next_page()
                urls = self.get_urls_in_page(station_info)
                all_urls += urls
                _print('URLS SAVED!', len(urls), 'TOTAL:', len(all_urls))
        all_urls = list(set(all_urls))
        return all_urls

    def click_order_by_time(self, retry=True):
        try:
            self.driver.find_element_by_link_text('最新上架').click()
        except Exception as e:
            _print('UNABLE TO LOCATE 最新上架')
            if(retry):
                self.check_driver(force=True)
                self.click_order_by_time(retry=False)

    # def click_order_by_metro(self):
    #     self.driver.find_element_by_link_text('按地铁线').click()

    # def get_all_lines(self):
    #     elms = self.driver.find_elements_by_xpath("//div[@class='filter__wrapper w1150']/ul[3]/*")
    #     return elms[1:]

    # def get_all_stations_in_line(self):
    #     elms = self.driver.find_elements_by_xpath("//div[@class='filter__wrapper w1150']/ul[4]/*")
    #     return elms[1:]

    # def get_all_station_urls(self, driver):
    #     stations = driver.find_elements_by_xpath(
    #         "//div[@class='filter__wrapper w1150']/ul[4]/li/a")
    #     filterd = list(map(lambda x: x.get_attribute('href'), stations[1:]))
    #     return filterd

    # def get_all_line_urls(self, driver):
    #     self.click_order_by_metro()
    #     lines = driver.find_elements_by_xpath(
    #         "//div[@class='filter__wrapper w1150']/ul[3]/li/a")
    #     filterd = list(map(lambda x: x.get_attribute('href'), lines[1:]))
    #     return filterd
    def getLocationInfo(self, apartment_info):
        result = getGeoInfo(apartment_info.get('city'), apartment_info.get(
            'district'), apartment_info.get('community_name'), apartment_info.get('bizcircle'))
        lng = result['location']['lng']
        lat = result['location']['lat']
        location_info = {
            'lng': lng,
            'lat': lat,
            'geo_info': result
        }
        return location_info

    def crawl_data_from_urls(self, urls, log=True):
        for url in tqdm(urls):
            driver = self._get(url)
            if log:
                _print('START', url)
            try:
                time.sleep(2)
                info = get_info_of_single_url(driver, url)
                if info is None:
                    db.delete_apartment_from_url(url)
                    if log:
                        _print('EXPIRED', url)
                else:
                    location_info = self.getLocationInfo(info)
                    doc = {**info, **location_info}
                    db.upsert_apartment(info.get('house_code'), doc)
                    if log:
                        _print("SUCCESS", url)
            except Exception as e:
                _error('ENCOUNTER ERR', url, Exception)

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

    def start_by_metro(self, latest=True):
        stations = db.find_all_stations()
        count = 0
        for station in stations:
            count += 1
            url = station.get('url')
            station_id = station.get('station_id')
            line_id = station.get('line_id')
            self._get(url)
            _print("START", station_id, line_id)
            if latest:
                self.click_order_by_time()
            all_urls = self.get_all_urls(station)
            _print(station_id, line_id,
                   'CRAWL URL BY STATION DONE, START CRAWL INFO')
            self.crawl_data_from_urls(all_urls, log=False)
            _print(station_id, line_id, 'DONE')
        print('DONE', count)

    def quit(self):
        self.driver.quit()
