from selenium.common.exceptions import InvalidSessionIdException, WebDriverException, NoSuchElementException, TimeoutException
from tqdm import tqdm
from time import sleep
from db import db
from common.utils.logger import logger
from common.utils.util import safely_get_url_from_driver
from common.proxy import connect_to_driver, setup_proxy_for_driver
from common.exceptions import ProxyBlockedException, UrlExistsException, ApartmentExpiredException, UrlCrawlerNoMoreNewUrlsException, TooManyTimesException
from random import shuffle
from common.locateElms import find_next_button, find_paging_elm, find_apartments_in_list, find_paging_elm_index, find_elm_of_latest_btn, get_num_of_apartment
from common.utils.constants import ERROR_AWAIT_TIME, URL_CRAWLER_TASK_DOWN_AWAIT_TIME, URL_CRAWLER_AWAIT_TIME
from datetime import datetime


def get_task_done_await_time():
    hour = datetime.now().hour
    if 0 <= hour <= 7:
        return (8-hour)*60*60

    if 20 <= hour <= 23:
        return (24-hour+8)*60*60

    return URL_CRAWLER_TASK_DOWN_AWAIT_TIME


class UrlCrawler(object):
    def __init__(self, city, city_url, source):
        self.driver = connect_to_driver()
        # shanghai
        self.city = city
        self.city_url = city_url
        # beike
        self.source = source

        self.opened_url_count = 0

        self.apartment_urls = []

        logger.info('INIT city: {} city url: {}'.format(city, city_url))

    def init_driver(self, test_url=None):
        return setup_proxy_for_driver(self.driver, test_url=test_url)

    def check_driver(self, open_last_page=True):
        logger.info('GET NEW PROXY')
        current_url = None
        if open_last_page:
            current_url = self.driver.current_url
        self.driver = self.init_driver(current_url)
        if open_last_page:
            self.driver.get(current_url)
        return self.driver

    def _get(self, url, times=0):
        if(times > 5):
            raise TooManyTimesException()
        try:
            self.driver.set_page_load_timeout(13)
            self.driver.get(url)
            self.opened_url_count += 1
            return self.driver
        except (TooManyTimesException, TimeoutException, WebDriverException) as e:
            logger.error('timeout tried times{} {}'.format(times, e))
            self.on_change_proxy(self.opened_url_count)

            self.opened_url_count = 0
            self.check_driver(open_last_page=False)

            return self._get(url, times=times + 1)

    def go_to_next_page(self):
        driver = self.driver
        try:
            driver.execute_script(
                "window.scrollTo(0, document.body.scrollHeight);")
            next_btn = find_next_button(driver)
            next_btn.click()
            logger.info('Click next button')
        except:
            '''
            next button not found
            '''
            try:
                logger.warning(
                    'Next button not found, try to click page index')

                current_page_elm = find_paging_elm(driver)
                total_page = current_page_elm.get_attribute('data-totalpage')
                current_page = current_page_elm.get_attribute('data-curpage')
                if total_page == current_page:
                    return logger.warning('Alreay at last page')
                current_page = int(current_page)
                elm = find_paging_elm_index(current_page+1)
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
            if 'apartment' not in url:
                try:
                    urls.append(url)
                except UrlExistsException:
                    pass
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
                    urls = self.get_urls_in_page(station_info)
                    url_saved = self.on_get_new_urls(urls, station_info)
                    if not url_saved:
                        raise UrlCrawlerNoMoreNewUrlsException()
                    logger.info('URLS SAVED! {} TOTAL: {}'.format(
                        len(urls), len(self.apartment_urls)))
                    if i < page_count - 1:
                        self.go_to_next_page()

                except ProxyBlockedException:
                    self.check_driver()

                except (InvalidSessionIdException, WebDriverException):
                    self.check_driver()

        return self.apartment_urls

    def on_change_proxy(self, opened_times):
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
            if db.insert_into_pool({
                'url': url.strip(),
                'city': self.city,
                'source': self.source,
                'station_info': station_info
            }):
                self.apartment_urls.append(url)
                saved_count += 1

        return saved_count

    def on_accomplish(self):
        logger.info('Total url length {}'.format(len(self.apartment_urls)))
        self.apartment_urls = []

    def click_order_by_time(self, retry=True):
        try:
            find_elm_of_latest_btn(self.driver).click()
            logger.info('Clicked 最新上架')
        except NoSuchElementException as e:
            logger.info('UNABLE TO LOCATE 最新上架')

    def on_open_station(self, station_info):
        try:
            priority = get_num_of_apartment(self.driver)
            db.update_priority_of_station(station_info, priority)
        except Exception as e:
            logger.exception(e)
            db.report_unexpected_error(e)

    def on_open_bizcircle(self, bizcircle_info):
        try:
            priority = get_num_of_apartment(self.driver)
            db.update_priority_of_bizcircle(bizcircle_info, priority)
        except Exception as e:
            logger.exception(e)
            db.report_unexpected_error(e)

    def start_by_url(self, url=None, station_info=None, bizcircle_info=None):
        try:
            logger.info('START')
            self._get(url or self.city_url)
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
            logger.info('No more new urls, start next')
        except (TimeoutException, WebDriverException, InvalidSessionIdException):
            logger.info('Session timeout')
            self.check_driver(open_last_page=False)
        except NoSuchElementException:
            logger.info('Elm not found')
        finally:
            self.on_accomplish()

    def start(self):
        '''
        loop 3 tasks
        '''
        all_tasks = [
            {
                'name': 'by city',
                'func': self.start_by_url,
                'args': [self.city_url]
            },
            {
                'name': 'by metro',
                'func': self.start_by_metro,
                'args': []
            },
            {
                'name': 'by district',
                'func': self.start_by_district,
                'args': []
            },
        ]
        shuffle(all_tasks)
        try:
            for task in all_tasks:
                logger.info('START {}'.format(task['name']))
                task['func'](*task['args'])
                logger.info('DONE {}, sleep {} min'.format(
                    task['name'], URL_CRAWLER_AWAIT_TIME/60))
                sleep(URL_CRAWLER_AWAIT_TIME)

            sleep_time = get_task_done_await_time()
            logger.info(
                'round done, sleep for {} hour'.format(sleep_time/3600))
            sleep(sleep_time)
        except RecursionError:
            exit(2)
        except Exception as e:
            logger.exception(e)
            db.report_unexpected_error(
                e, safely_get_url_from_driver(self.driver))
            sleep(ERROR_AWAIT_TIME)
        finally:
            self.start()

    def start_by_district(self):
        bizcircles = db.find_all_bizcircles(self.city)
        count = 0
        logger.info('start_by_district')
        for bizcircle in bizcircles:
            count += 1
            url = bizcircle.get('bizcircle_url')
            bizcircle_name = bizcircle.get('bizcircle_name')
            logger.info(f"START {bizcircle_name}")
            self.start_by_url(url, bizcircle_info=bizcircle)
            logger.info(f'{bizcircle_name}, DONE, {count}')

    def start_by_metro(self):
        '''
        by metro station
        '''
        stations = db.find_all_stations(self.city)
        count = 0
        logger.info('start_by_metro')
        for station in stations:
            count += 1
            url = station.get('url')
            station_name = station.get('station_name')
            logger.info(f"START {station_name}")
            self.start_by_url(url, station_info=station)
            logger.info(f'{station_name}, DONE, {count}')

    def quit(self):
        try:
            self.driver.quit()
        except Exception as e:
            logger.error(f'QUIT ERROR {e}')
