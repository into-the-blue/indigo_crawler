from selenium.common.exceptions import InvalidSessionIdException, WebDriverException
from tqdm import tqdm
from time import sleep
from db import db
from common.utils.logger import logger
from common.proxy import connect_to_driver, setup_proxy_for_driver
from common.exceptions import ProxyBlockedException, UrlExistsException, ApartmentExpiredException, NoTaskException
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
        # except WebDriverException:
        #     self.check_driver()
        except ApartmentExpiredException:
            logger.info('Url expired')
            db.task_expired(task)
        except Exception as e:
            logger.error('Unexcepected error {}'.format(e))
            db.update_failure(task, self.driver.page_source)

    def start(self):
        try:
            self.start_one_url()
            sleep(2)
            self.start()
        except NoTaskException:
            logger.info('No task found')
            sleep(60*5)
            self.start()

    def quit(self):
        try:
            self.driver.quit()
        except Exception as e:
            logger.error(f'QUIT ERROR {e}')
