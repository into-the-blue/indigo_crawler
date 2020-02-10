from selenium.common.exceptions import InvalidSessionIdException, WebDriverException, TimeoutException
from tqdm import tqdm
from time import sleep
from db import db
from common.utils.logger import logger
from common.utils.constants import ERROR_AWAIT_TIME, DATA_VALIDATOR_AWAIT_TIME
from common.proxy import connect_to_driver, setup_proxy_for_driver
from common.exceptions import ProxyBlockedException, UrlExistsException, ApartmentExpiredException, NoTaskException, ValidatorInvalidValueException, TooManyTimesException
from random import shuffle
from common.locateElms import find_next_button, find_paging_elm, find_apartments_in_list
from validator import examine_apartment
import traceback


class DataValidator(object):
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
            raise TooManyTimesException()
        try:
            self.driver.set_page_load_timeout(10)
            self.driver.get(url)
            self.opened_url_count += 1
            return self.driver
        except (TooManyTimesException, TimeoutException) as e:
            logger.error('PROXY BLOCKED {}'.format(e))
            self.on_change_proxy(self.opened_url_count)

            self.opened_url_count = 0
            self.check_driver(open_last_page=False)

            return self._get(url, times=times + 1)

    def on_change_proxy(self, opened_times):
        db.report_error(
            'proxy_opened_urls',
            {
                'count': opened_times
            }
        )

    def start(self):
        logger.info('START')
        staging_apartment = db.get_unchecked()
        try:
            if not staging_apartment:
                raise NoTaskException()
            examine_apartment(staging_apartment)
            db.on_pass_validation(staging_apartment)
            logger.info('pass validation')
            self.start()

        except NoTaskException:
            logger.info('No task to run, sleep for {} min'.format(
                DATA_VALIDATOR_AWAIT_TIME/60))
            sleep(DATA_VALIDATOR_AWAIT_TIME)
            self.start()

        except ValidatorInvalidValueException as e1:
            logger.info('Found invalid value')
            invalid_values = e1.args[1]
            db.report_invalid_value(staging_apartment, invalid_values)
            self.start()

        except Exception as e:
            logger.exception(e)
            db.report_unexpected_error(e, staging_apartment.get(
                'house_url') if staging_apartment else None)
            sleep(ERROR_AWAIT_TIME)
            self.start()

    def quit(self):
        try:
            self.driver.quit()
        except Exception as e:
            logger.error(f'QUIT ERROR {e}')
