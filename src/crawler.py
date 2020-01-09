from selenium import webdriver
from selenium.common.exceptions import InvalidSessionIdException
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
from proxy_pool import renew_or_create_driver
from db.db import db
from utils.util import logger
from hooks import DefaultHooker, HookHandler, FormatData
from exceptions import UrlExistsException, ApartmentExpiredException
from baiduMap.getCoordinates import get_location_info_from_apartment_info

hooks = [DefaultHooker, FormatData]
hookHandler = HookHandler(hooks)


class GrapPage(object):
    def __init__(self, city, city_url):
        self.driver = self.init_driver()
        self.city = city
        self.driver_period = 0
        self.city_url = city_url

    def init_driver(self, test_url=None):
        return renew_or_create_driver(test_url=test_url)

    def check_driver(self, open_last_page=True,  force=False):
        if self.driver_period >= 10 or force:
            logger.info('GET NEW PROXY')

            current_url = self.driver.current_url
            try:
                self.driver.close()
            except:
                pass

            self.driver = self.init_driver(current_url)
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
            logger.error('PROXY BLOCKED {}'.format(e))
            # delete_proxy()
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
                logger.info('BLOCKED, CHANGE PROXY')
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
                # HOOK on get url
                try:
                    hookHandler('on_get_url', url, station_info)
                    urls.append(url)
                except UrlExistsException:
                    pass
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
                logger.info('URLS SAVED! {} TOTAL: {}'.format(
                    len(urls), len(all_urls)))
        all_urls = list(set(all_urls))
        return all_urls

    def click_order_by_time(self, retry=True):
        try:
            self.driver.find_element_by_link_text('最新上架').click()
        except Exception as e:
            logger.info('UNABLE TO LOCATE 最新上架')
            if(retry):
                self.check_driver(force=True)
                self.click_order_by_time(retry=False)

    def crawl_data_from_urls(self, urls, log=True):
        for url in tqdm(urls):
            driver = self._get(url)
            if log:
                logger.info(f'START {url}')
            try:
                time.sleep(2)
                info = get_info_of_single_url(driver, url)
                # HOOK on_get_apartment_info
                hookHandler('on_get_apartment_info', info, get_location_info_from_apartment_info(
                    info) if info else None)
                if log:
                    logger.info(f"SUCCESS {url}")

                # DEPRECATED
                # if info is None:
                    # db.delete_apartment_from_url(url)
                    # if log:
                    #     logger.info('EXPIRED', url)
                # else:
                    # location_info = self.getLocationInfo(info)
                    # doc = {**info, **location_info}
                    # db.upsert_apartment(info.get('house_code'), doc)
                    # if log:
                    #     logger.info("SUCCESS", url)
            except ApartmentExpiredException:
                if log:
                    logger.info(f'EXPIRED {url}')
            except Exception as e:
                logger.error(f'ENCOUNTER ERR {url} ERROR: {e}')

    def start_filling_missing_info(self):
        logger.info('START FILLING')
        all_urls = db.find_missing_apartments()
        logger.info('CRAWL URL DONE, START CRAWL INFO')
        self.crawl_data_from_urls(all_urls)
        logger.info('DONE')

    def start(self):
        logger.info('START')
        self.driver.get(self.city_url)
        all_urls = self.get_all_urls()
        logger.info('CRAWL URL DONE, START CRAWL INFO')
        self.crawl_data_from_urls(all_urls)
        logger.info('DONE')

    def start_by_latest(self):
        self.driver.get(self.city_url)
        self.click_order_by_time()
        urls = self.get_all_urls()
        self.crawl_data_from_urls(urls)

    def start_by_metro(self, latest=True, reverse=False):
        stations = db.find_all_stations()
        if(reverse):
            stations.reverse()
        count = 0
        error_count = 0
        for station in stations:
            try:
                count += 1
                url = station.get('url')
                station_id = station.get('station_id')
                station_name = station.get('station_name')
                line_ids = station.get('line_ids')
                self._get(url)
                logger.info(f"START {station_id} {station_name}")
                if latest:
                    self.click_order_by_time()
                all_urls = self.get_all_urls(station)
                logger.info(
                    f'{station_id}, {station_name} CRAWL URL BY STATION DONE, START CRAWL INFO')
                self.crawl_data_from_urls(all_urls, log=False)
                logger.info(f'{station_id}, {station_name}, DONE, {count}')
            except InvalidSessionIdException:
                if(error_count >= 10):
                    raise e
                error_count += 1
                logger.error(f'start_by_metro Invalid Session Id')
                self.check_driver(force=True)

            except Exception as e:
                if(error_count >= 10):
                    raise e
                error_count += 1
                logger.error(f'start_by_metro {e}')
        print('DONE')

    def quit(self):
        try:
            self.driver.quit()
        except Exception as e:
            logger.error(f'QUIT ERROR {e}')
