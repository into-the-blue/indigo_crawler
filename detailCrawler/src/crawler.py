from selenium.common.exceptions import InvalidSessionIdException, WebDriverException, TimeoutException
from tqdm import tqdm
from time import sleep
from db import db
from common.utils.logger import logger
from common.utils.util import safely_get_url_from_driver
from common.utils.constants import DETAIL_CRAWLER_AWAIT_TIME, ERROR_AWAIT_TIME, TASK_DONE_AWAIT_TIME
from common.proxy import connect_to_driver, setup_proxy_for_driver
from common.exceptions import ProxyBlockedException, UrlExistsException, ApartmentExpiredException, NoTaskException, TooManyTimesException
from random import shuffle
from common.locateElms import find_next_button, find_paging_elm, find_apartments_in_list
from crawlSingleUrl import get_info_of_single_url


class DetailCrawler(object):
    def __init__(self, city, city_url, source):
        self.driver = connect_to_driver()
        # shanghai
        self.city = city
        self.city_url = city_url
        # beike
        self.source = source

        self.opened_url_count = 0

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
            raise TooManyTimesException()
        try:
            self.driver.set_page_load_timeout(20)
            self.driver.get(url)
            self.opened_url_count += 1
            return self.driver
        except (TimeoutException, TooManyTimesException) as e:
            logger.error('timeout tried times {} {}'.format(times, e))
            self.on_change_proxy(self.opened_url_count)

            self.opened_url_count = 0
            self.check_driver(open_last_page=False)

            return self._get(url, times=times + 1)

    def on_change_proxy(self, opened_times):
        db.report_error(
            'proxy_opened_urls',
            None,
            {
                'count': opened_times
            }
        )

    def start_one_url(self):
        task = db.get_one_task()
        if not task:
            raise NoTaskException()
        try:
            self._get(task.get('url'))
            logger.info('Url opened')
            info = get_info_of_single_url(self.driver, task.get('url'))
            logger.info('Data get')
            db.insert_into_staing(task, info)
            sleep(2)
            self.start_one_url()
        except ApartmentExpiredException:
            logger.info('Url expired')
            db.task_expired(task)
            self.start_one_url()
        except Exception as e:
            logger.exception(e)
            db.update_failure(task, e, self.driver.page_source)
            raise e

    def start_fill_missing(self):
        apartment = db.get_missing_info()
        if not apartment:
            raise NoTaskException()
        try:
            self._get(apartment.get('house_url'))
            logger.info('Url opened')
            info = get_info_of_single_url(
                self.driver, apartment.get('house_url'))
            logger.info('Data get')
            db.update_missing_info(apartment, info)
            sleep(2)
            self.start_fill_missing()
        except ApartmentExpiredException:
            logger.info('Url expired')
            db.update_missing_info(apartment, {
                'expired': True,
            })
            sleep(2)
            self.start_fill_missing()
        except Exception as e:
            logger.exception(e)
            raise e

    def start(self):
        try:
            self.start_one_url()
            self.start_fill_missing()
            sleep(DETAIL_CRAWLER_AWAIT_TIME)
        except NoTaskException:
            logger.info('No task found')
            sleep(TASK_DONE_AWAIT_TIME)
        except Exception as e:
            logger.exception(e)
            db.report_unexpected_error(
                e, safely_get_url_from_driver(self.driver))
            sleep(ERROR_AWAIT_TIME)
        finally:
            self.start()

    def quit(self):
        try:
            self.driver.quit()
        except Exception as e:
            logger.error(f'QUIT ERROR {e}')
