from selenium.common.exceptions import InvalidSessionIdException, WebDriverException, NoSuchElementException, TimeoutException
from tqdm import tqdm
from time import sleep
from db import mongo
from utils.logger import logger
from utils.util import safely_get_url_from_driver
from exceptions import ProxyBlockedException, UrlExistsException, ApartmentExpiredException, UrlCrawlerNoMoreNewUrlsException, TooManyTimesException
from random import shuffle
from locateElms import find_next_button, find_paging_elm, find_apartments_in_list, find_paging_elm_index, find_elm_of_latest_btn, get_num_of_apartment
from utils.constants import ERROR_AWAIT_TIME, URL_CRAWLER_TASK_DOWN_AWAIT_TIME, URL_CRAWLER_AWAIT_TIME, URL_CRAWLER_TASK_BY_BIZCIRCLE, URL_CRAWLER_TASK_BY_METRO, URL_CRAWLER_TASK_BY_LATEST
from datetime import datetime
from browser import BaseWebDriver


def get_task_done_await_time():
    hour = datetime.now().hour
    if 0 <= hour <= 7:
        return (8-hour)*60*60

    if 20 <= hour <= 23:
        return (24-hour+8)*60*60

    return URL_CRAWLER_TASK_DOWN_AWAIT_TIME


class UrlCrawler(BaseWebDriver):
    def __init__(self, on_finish=None):
        super().__init__()
        self.apartment_urls = []
        self.on_finish = on_finish

    def setup_city_and_source(self, city, source):
        self.city = city
        self.source = source

    def go_to_next_page(self):
        try:
            self.driver.execute_script(
                "window.scrollTo(0, document.body.scrollHeight);")
            next_btn = find_next_button(self.driver)
            next_btn.click()
            logger.info('Click next button')
        except:
            '''
            next button not found
            '''
            try:
                logger.warning(
                    'Next button not found, try to click page index')

                current_page_elm = find_paging_elm(self.driver)
                total_page = current_page_elm.get_attribute('data-totalpage')
                current_page = current_page_elm.get_attribute('data-curpage')
                if total_page == current_page:
                    return logger.warning('Alreay at last page')
                current_page = int(current_page)
                elm = find_paging_elm_index(self.driver, current_page+1)
                elm.click()
                logger.info('Click page index success')
            except:
                # logger.info('BLOCKED, CHANGE PROXY')
                raise ProxyBlockedException()

    def read_page_count(self):
        '''
            read page count of url
        '''
        try:
            num = get_num_of_apartment(self.driver)
            if not num:
                return 0
            elm = find_paging_elm(self.driver)
            page_count = elm.get_attribute('data-totalpage')
            logger.info('Page read, total: {}'.format(page_count))
            return int(page_count)
        except NoSuchElementException:
            logger.info('Page info not found')
            return 0

    def get_urls_in_page(self, station_info=None):
        '''
            get urls in one page
        '''
        urls = []
        elms = find_apartments_in_list(self.driver)

        logger.info('elments in page {}'.format(len(elms)))
        for i in elms:
            url = i.get_attribute('href')
            if 'apartment' not in url and '广告' not in i.text:
                urls.append(url)
        return urls

    def get_all_urls(self, station_info=None):
        '''
            loop all pages, get all urls
        '''
        page_count = self.read_page_count()
        logger.info('total pages {}'.format(page_count))
        if page_count > 0:
            for i in range(page_count):
                try:
                    sleep(2)
                    logger.info('Get urls in page, current {}'.format(i+1))
                    self.driver.execute_script(
                        "window.scrollTo(0, document.body.scrollHeight);")
                    urls = self.get_urls_in_page(station_info)
                    url_saved = self.on_get_new_urls(urls, station_info)
                    if not url_saved:
                        raise UrlCrawlerNoMoreNewUrlsException()
                    logger.info('URLS SAVED! {} TOTAL: {}'.format(
                        len(urls), len(self.apartment_urls)))
                    if i < page_count - 1:
                        self.go_to_next_page()
                except ProxyBlockedException:
                    self.renew_driver()

                except (InvalidSessionIdException, WebDriverException):
                    self.renew_driver()

        return self.apartment_urls

    def on_change_proxy(self, opened_times):
        '''
        triggered when change proxy
        '''
        pass
        # db.report_error(
        #     'proxy_opened_urls',
        #     {
        #         'count': opened_times
        #     }
        # )

    def on_get_new_urls(self, urls, station_info):
        '''
        on get url, save them into db
        '''
        saved_count = 0
        for url in urls:
            if mongo.insert_into_pool({
                'url': url.strip(),
                'city': self.city,
                'source': self.source,
                'station_info': station_info
            }):
                self.apartment_urls.append(url)
                saved_count += 1

        return saved_count

    def on_accomplish(self, taskname=None):
        num_of_new_apartments = len(self.apartment_urls)
        logger.info('Total url length {}'.format(num_of_new_apartments))
        self.apartment_urls = []
        self.on_finish and self.on_finish(taskname, num_of_new_apartments)
        self.quit()

    def click_order_by_time(self):
        '''
        ckick order by time
        '''
        try:
            find_elm_of_latest_btn(self.driver).click()
            logger.info('Clicked 最新上架')
        except NoSuchElementException:
            logger.info('UNABLE TO LOCATE 最新上架')

    def on_open_station(self, station_info):
        '''
        get apartments count
        '''
        try:
            priority = get_num_of_apartment(self.driver)
            mongo.update_priority_of_station(station_info, priority)
        except NoSuchElementException:
            logger.info('Unable to get apartment count')
            raise
        except Exception as e:
            logger.exception(e)
            mongo.report_unexpected_error_url_crawler(e)

    def on_open_bizcircle(self, bizcircle_info):
        '''
        get apartments count
        '''
        try:
            priority = get_num_of_apartment(self.driver)
            mongo.update_priority_of_bizcircle(bizcircle_info, priority)
        except NoSuchElementException:
            logger.info('Unable to get apartment count')
            raise
        except Exception as e:
            logger.exception(e)
            mongo.report_unexcepted_error_url_crawler(e)

    def start_by_url(self, url, taskname=URL_CRAWLER_TASK_BY_LATEST, station_info=None, bizcircle_info=None):
        print(url)
        try:
            logger.info('START {}'.format(url))
            self.get(url)
            logger.info('Url opened')
            if station_info:
                self.on_open_station(station_info)
            if bizcircle_info:
                self.on_open_bizcircle(bizcircle_info)
            self.click_order_by_time()
            logger.info('Clicked order by time')
            self.get_all_urls(station_info)
            logger.info('start DONE')
        except UrlCrawlerNoMoreNewUrlsException:
            logger.info('No more new urls')
        except NoSuchElementException:
            logger.info('Elm not found')
            sleep(ERROR_AWAIT_TIME)
        except (TimeoutException, WebDriverException, InvalidSessionIdException):
            logger.info('Session timeout')
            self.renew_driver(open_last_page=False)
        finally:
            self.on_accomplish(taskname)

    def start_by_district(self, city):
        bizcircles = mongo.find_all_bizcircles(city)
        count = 0
        logger.info('start_by_district {}'.format(city))
        for bizcircle in bizcircles:
            count += 1
            url = bizcircle.get('bizcircle_url')
            bizcircle_name = bizcircle.get('bizcircle_name')
            logger.info(f"START {bizcircle_name}")
            self.start_by_url(
                url, taskname=URL_CRAWLER_TASK_BY_BIZCIRCLE, bizcircle_info=bizcircle)
            logger.info(f'{bizcircle_name}, DONE, {count}')

    def start_by_metro(self, city):
        '''
        by metro station
        '''
        stations = mongo.find_all_stations(city)
        count = 0
        logger.info('start_by_metro: {}'.format(city))
        for station in stations:
            count += 1
            url = station.get('url')
            station_name = station.get('station_name')
            logger.info(f"START {station_name}")
            self.start_by_url(
                url, taskname=URL_CRAWLER_TASK_BY_METRO, station_info=station)
            logger.info(f'{station_name}, DONE, {count}')
