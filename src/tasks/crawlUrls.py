from selenium.common.exceptions import InvalidSessionIdException, WebDriverException, NoSuchElementException, TimeoutException
from tqdm import tqdm
from time import sleep
from db import mongo
from utils.logger import logger
from utils.util import safely_get_url_from_driver
from exceptions import ProxyBlockedException, UrlExistsException, ApartmentExpiredException, UrlCrawlerNoMoreNewUrlsException, TooManyTimesException, IpBlockedException
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
        self.city = None
        self.timesOfOUrls = 0

    def __del__(self):
        class_name = self.__class__.__name__
        self.quit()
        logger.info('[{}] detroyed'.format(class_name))

    def setup_city_and_source(self, city_obj):
        self.city = city_obj.get('city')
        self.source = city_obj.get('source')
        self.city_obj = city_obj

    def go_to_next_page(self):
        try:
            self.driver.execute_script(
                "window.scrollTo(0, document.body.scrollHeight);")
            next_btn = find_next_button(self.driver)
            next_btn.click()
            logger.info(
                '[{}] [UrlCrawler] Click next button'.format(self.city))
        except:
            '''
            next button not found
            '''
            try:
                logger.warning(
                    '[{}] [UrlCrawler] Next button not found, try to click page index'.format(self.city))

                current_page_elm = find_paging_elm(self.driver)
                total_page = current_page_elm.get_attribute('data-totalpage')
                current_page = current_page_elm.get_attribute('data-curpage')
                if total_page == current_page:
                    return logger.warning('[{}] [UrlCrawler] Alreay at last page'.format(self.city))
                current_page = int(current_page)
                elm = find_paging_elm_index(self.driver, current_page+1)
                elm.click()
                logger.info(
                    '[{}] [UrlCrawler] Click page index success'.format(self.city))
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
            logger.info('[{}] [UrlCrawler] Page read, total: {}'.format(
                self.city, page_count))
            return int(page_count)
        except NoSuchElementException:
            logger.info(
                '[{}] [UrlCrawler] Page info not found'.format(self.city))
            return 0

    def get_urls_in_page(self, station_info=None):
        '''
            get urls in one page
        '''
        urls = []
        elms = find_apartments_in_list(self.driver)

        logger.info('[{}] [UrlCrawler] elments in page {}'.format(
            self.city, len(elms)))
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
        logger.info('[{}] [UrlCrawler] total pages {}'.format(
            self.city, page_count))
        if page_count > 0:
            for i in range(page_count):
                try:
                    sleep(2)
                    logger.info(
                        '[{}] [UrlCrawler] Get urls in page, current {}'.format(self.city, i+1))
                    self.driver.execute_script(
                        "window.scrollTo(0, document.body.scrollHeight);")
                    urls = self.get_urls_in_page(station_info)
                    url_saved = self.on_get_new_urls(urls, station_info)
                    if not url_saved:
                        # times += 1
                        self.timesOfOUrls = self.timesOfOUrls+1
                        if self.timesOfOUrls >= 3:
                            raise UrlCrawlerNoMoreNewUrlsException()
                    else:
                        # reset times
                        self.timesOfOUrls = 0

                    logger.info('[{}] [UrlCrawler] URLS SAVED! {} TOTAL: {}'.format(self.city,
                                                                                    url_saved, len(self.apartment_urls)))
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

    def on_get_new_urls(self, urls, station_info=None):
        '''
        on get url, save them into db
        '''
        saved_count = 0
        metadata = [{
            'url': url.strip(),
            'city': self.city,
            'source': self.source,
            'station_info': station_info
        } for url in urls]

        urls_saved = mongo.insert_into_pool(metadata)
        self.apartment_urls.extend(urls_saved)
        num_of_saved_urls = len(urls_saved)
        return num_of_saved_urls

    def on_accomplish(self, taskname=None, job_id=None):
        num_of_new_apartments = len(self.apartment_urls)
        logger.info('[{}] [UrlCrawler] Total url length {}'.format(
            self.city, num_of_new_apartments))
        self.apartment_urls = []
        if taskname == URL_CRAWLER_TASK_BY_LATEST:
            self.on_finish and self.on_finish(
                taskname, url_count=num_of_new_apartments, city=self.city_obj, job_id=job_id)
            self.quit()

    def click_order_by_time(self):
        '''
        ckick order by time
        '''
        try:
            find_elm_of_latest_btn(self.driver).click()
            logger.info('[{}] [UrlCrawler] Clicked 最新上架'.format(self.city))
        except NoSuchElementException:
            logger.info('[{}] UNABLE TO LOCATE 最新上架'.format(self.city))

    def on_open_station(self, station_info):
        '''
        get apartments count
        '''
        try:
            priority = get_num_of_apartment(self.driver)
            mongo.update_priority_of_station(station_info.get('_id'), priority)
        except NoSuchElementException:
            logger.info(
                '[{}] [UrlCrawler] Unable to get apartment count'.format(self.city))
            raise
        except Exception as e:
            logger.error(
                '[{}] [UrlCrawler] [on_open_station] err'.format(self.city))
            logger.exception(e)
            mongo.report_unexpected_error_url_crawler(e)

    def on_open_bizcircle(self, bizcircle_info):
        '''
        get apartments count
        '''
        try:
            priority = get_num_of_apartment(self.driver)
            mongo.update_priority_of_bizcircle(
                bizcircle_info.get('_id'), priority)
        except NoSuchElementException:
            logger.info(
                '[{}] [UrlCrawler] Unable to get apartment count'.format(self.city))
            raise
        except Exception as e:
            logger.error(
                '[{}] [UrlCrawler] [on_open_bizcircle]'.format(self.city))
            logger.exception(e)
            mongo.report_unexcepted_error_url_crawler(e)

    def start_by_url(self, url, by_latest=True, job_id=None, taskname=URL_CRAWLER_TASK_BY_LATEST, station_info=None, bizcircle_info=None):
        logger.info('[{}] [UrlCrawler] PRAMS {} {} {}'.format(
            self.city, job_id, taskname, station_info or bizcircle_info))
        try:
            logger.info('[{}] [UrlCrawler] START {}'.format(self.city, url))
            self.get(url)
            logger.info('[{}] [UrlCrawler] Url opened'.format(self.city))
            if station_info:
                self.on_open_station(station_info)
            if bizcircle_info:
                self.on_open_bizcircle(bizcircle_info)
            if by_latest:
                self.click_order_by_time()
            logger.info(
                '[{}] [UrlCrawler] Clicked order by time'.format(self.city))
            self.get_all_urls(station_info)
            logger.info('[{}] [UrlCrawler] start DONE'.format(self.city))
        except UrlCrawlerNoMoreNewUrlsException:
            logger.info('[{}] [UrlCrawler] No more new urls'.format(self.city))
        except NoSuchElementException:
            logger.warning('[{}] [UrlCrawler] Elm not found'.format(self.city))
            sleep(ERROR_AWAIT_TIME)
        except (TimeoutException, WebDriverException, InvalidSessionIdException):
            logger.warning(
                '[{}] [UrlCrawler] Session timeout'.format(self.city))
            self.renew_driver()
        except IpBlockedException:
            logger.warning(
                '[{}] [UrlCrawler] IP blocked by target'.format(self.city))
            mongo.report_error_ip_blocked(url)
            self.renew_driver()
        except Exception as e:
            logger.warning(
                '[{}] [UrlCrawler] unexpected error'.format(self.city, e))
            logger.exception(e)
            mongo.report_unexcepted_error_url_crawler(e)
        finally:
            self.on_accomplish(taskname, job_id)

    def start_by_district(self, city, job_id):
        bizcircles = mongo.find_all_bizcircles(city)
        count = 0
        logger.info(
            '[{}] [UrlCrawler] start_by_district {}'.format(city, city))
        for bizcircle in bizcircles:
            count += 1
            url = bizcircle.get('bizcircle_url')
            bizcircle_name = bizcircle.get('bizcircle_name')
            logger.info(f"[{city}] [UrlCrawler] START {bizcircle_name}")
            self.start_by_url(
                url, taskname=URL_CRAWLER_TASK_BY_BIZCIRCLE, bizcircle_info=bizcircle)
            logger.info(
                f'[{city}] [UrlCrawler] {bizcircle_name}, DONE, {count}')
        mongo.on_job_done(job_id, city=city)

    def start_by_metro(self, city, job_id):
        '''
        by metro station
        '''
        stations = mongo.find_all_stations(city)
        count = 0
        logger.info('[{}] [UrlCrawler] start_by_metro: {}'.format(city, city))
        for station in stations:
            count += 1
            url = station.get('url')
            station_name = station.get('station_name')
            logger.info(f"[{city}] [UrlCrawler] START {station_name}")
            self.start_by_url(
                url, taskname=URL_CRAWLER_TASK_BY_METRO, station_info=station)
            logger.info(f'[{city}] [UrlCrawler] {station_name}, DONE, {count}')
        mongo.on_job_done(job_id, city=city)
