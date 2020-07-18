from .connect import connect_to_driver, setup_proxy_for_driver
from utils.logger import logger
from exceptions import TooManyTimesException, IpBlockedException
from selenium.common.exceptions import InvalidSessionIdException, WebDriverException, NoSuchElementException, TimeoutException
import os

# cn-sh-1 / fc-sg-1
SERVER_NAME = os.getenv('SERVER_NAME', 'unknown')


class BaseWebDriver(object):
    def __init__(self):
        self.__driver = None
        self.opened_url_count = 0
        self.connected = False

    def renew_driver(self, open_last_page=True):
        logger.info('RENEW DRIVER')
        current_url = self.__driver.current_url if self.__driver is not None else None
        self.__driver = setup_proxy_for_driver(
            self.__driver, test_url=current_url)
        if open_last_page and current_url:
            self.__driver.get(current_url)
        return self.__driver

    @property
    def driver(self):
        if self.__driver is None:
            self.__driver = connect_to_driver()
            self.connected = True
            setup_proxy_when_init_driver = 'cn' not in SERVER_NAME.lower()
            if setup_proxy_when_init_driver:
                self.__driver = setup_proxy_for_driver(self.__driver)
        return self.__driver

    def get(self, url, times=0):
        if(times > 5):
            raise TooManyTimesException()
        try:
            self.driver.set_page_load_timeout(13)
            self.driver.get(url)
            self.opened_url_count += 1
            if self.driver.title == '人机认证':
                raise IpBlockedException()
            return self.driver
        except (TooManyTimesException, TimeoutException, WebDriverException) as e:
            logger.error('timeout tried times {} {}'.format(times, e))
            self.opened_url_count = 0
            self.renew_driver(open_last_page=False)
            return self.get(url, times=times + 1)

    def quit(self):
        if not self.connected:
            return
        try:
            logger.info('DRIVER QUIT')
            self.driver.delete_all_cookies()
            self.driver.quit()
            self.connected = False
        except Exception as e:
            logger.error(f'QUIT DRIVER ERROR {e}')
