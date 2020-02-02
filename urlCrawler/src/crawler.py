from selenium.common.exceptions import InvalidSessionIdException, WebDriverException
from tqdm import tqdm
from time import sleep
from db import db
from common.utils.logger import logger
from common.proxy import connect_to_driver, setup_proxy_for_driver
from common.exceptions import ProxyBlockedException, UrlExistsException, ApartmentExpiredException
from random import shuffle
from common.locateElms import find_next_button, find_paging_elm, find_apartments_in_list


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
        current_url = self.driver.current_url
        self.driver = self.init_driver(current_url)
        if open_last_page:
            self.driver.get(current_url)
        return self.driver

    def _get(self, url, times=0):
        if(times > 5):
            raise Exception('TOO MANY TIMES')
        try:
            self.driver.set_page_load_timeout(10)
            self.driver.get(url)
            self.opened_url_count += 1
            return self.driver
        except Exception as e:
            logger.error('PROXY BLOCKED {}'.format(e))
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
                elm = current_page_elm.find_element_by_xpath(
                    "./a[@data-page={}]".format(current_page+1))
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
            elm = find_paging_elm(self.driver)
            page_count = elm.get_attribute('data-totalpage')
            logger.info('Page read, total: {}'.format(page_count))
            return int(page_count)
        except:
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
                    # logger.info('HOOK on_get_url')
                    # hookHandler('on_get_url', url, station_info)
                    urls.append(url)
                except UrlExistsException:
                    pass
        return urls

    def get_all_urls(self, station_info=None):
        '''
        loop all pages, get all urls
        '''
        page_count = self.read_page_count()
        if page_count > 0:
            for i in range(page_count):
                try:
                    sleep(2)
                    logger.info('Get urls in page, current {}'.format(i+1))
                    urls = self.get_urls_in_page(station_info)
                    self.on_get_new_urls(urls, station_info)
                    logger.info('URLS SAVED! {} TOTAL: {}'.format(
                        len(urls), len(self.apartment_urls)))
                    if i < page_count - 1:
                        self.go_to_next_page()

                except ProxyBlockedException:
                    self.check_driver()

                except WebDriverException:
                    self.check_driver()

        return self.apartment_urls

    def on_change_proxy(self, opened_times):
        pass

    def on_get_new_urls(self, urls, station_info):
        '''
        on get url, save them into db
        '''
        for url in urls:
            if db.insert_into_pool({
                'url': url.strip(),
                'city': self.city,
                'source': self.source,
                'station_info': station_info
            }):
                self.apartment_urls.append(url)

    def on_accomplish(self):
        logger.info('Total url length {}'.format(len(self.apartment_urls)))

    def click_order_by_time(self, retry=True):
        try:
            self.driver.find_element_by_link_text('最新上架').click()
        except Exception as e:
            logger.info('UNABLE TO LOCATE 最新上架')

    def start(self, url=None, station_info=None):
        logger.info('START')
        try:
            self._get(url or self.city_url)
            logger.info('Url opened')
            self.click_order_by_time()
            logger.info('Clicked order by time')
            self.get_all_urls(station_info)
            self.on_accomplish()
            logger.info('start DONE')
        except Exception as e:
            raise e

    def start_by_metro(self):
        stations = db.find_all_stations(self.city)
        shuffle(stations)
        count = 0
        logger.info('start_by_metro')
        for station in stations:
            # try:
            count += 1
            url = station.get('url')
            station_id = station.get('station_id')
            station_name = station.get('station_name')
            line_ids = station.get('line_ids')
            logger.info(f"START {station_id} {station_name}")
            self.start(url, station)
            logger.info(f'{station_id}, {station_name}, DONE, {count}')
            # except InvalidSessionIdException:
            #     if(error_count >= 10):
            #         raise e
            #     error_count += 1
            #     logger.error(f'start_by_metro Invalid Session Id')
            #     self.check_driver()

            # except Exception as e:
            #     if(error_count >= 10):
            #         raise e
            #     error_count += 1
            #     logger.error(f'start_by_metro {e}')

    def quit(self):
        try:
            self.driver.quit()
        except Exception as e:
            logger.error(f'QUIT ERROR {e}')
