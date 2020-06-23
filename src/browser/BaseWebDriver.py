from .connect import connect_to_driver, setup_proxy_for_driver
from utils.logger import logger
from exceptions import TooManyTimesException
from selenium.common.exceptions import InvalidSessionIdException, WebDriverException, NoSuchElementException, TimeoutException


class BaseWebDriver(object):
    def __init__(self):
        self.__driver = None
        self.opened_url_count = 0
        self.connected = False

    def renew_driver(self, test_url=None):
        self.__driver =  setup_proxy_for_driver(self.driver, test_url=test_url)
        return self.__driver

    @property
    def driver(self):
        if self.__driver is None:
            self.__driver = connect_to_driver()
            self.connected = True
        return self.__driver

    def get(self, url, times=0):
        if(times > 5):
            raise TooManyTimesException()
        try:
            self.driver.set_page_load_timeout(13)
            self.driver.get(url)
            self.opened_url_count += 1
            return self.driver
        except (TooManyTimesException, TimeoutException, WebDriverException) as e:
            logger.error('timeout tried times{} {}'.format(times, e))
            self.opened_url_count = 0
            self.renew_driver()
            return self.get(url, times=times + 1)

    def quit(self):
        if not self.connected:
            return
        try:
            self.driver.quit()
            self.connected = False
        except Exception as e:
            logger.error(f'QUIT DRIVER ERROR {e}')
