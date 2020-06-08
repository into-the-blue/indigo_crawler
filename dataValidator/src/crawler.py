from selenium.common.exceptions import InvalidSessionIdException, WebDriverException, TimeoutException
from tqdm import tqdm
from time import sleep
from db import db
from common.utils.logger import logger
from common.utils.constants import ERROR_AWAIT_TIME, DATA_VALIDATOR_AWAIT_TIME
from common.proxy import connect_to_driver, setup_proxy_for_driver
from common.exceptions import ProxyBlockedException, UrlExistsException, ApartmentExpiredException, NoTaskException, ValidatorInvalidValueException, TooManyTimesException
from random import shuffle
from common.locateElms import find_apartments_in_list
from validator import examine_apartment
import traceback
import requests
import os

INDIGO_ACCESS_TOKEN = os.environ.get('INDIGO_ACCESS_TOKEN')
API_DOMAIN = os.environ.get('API_DOMAIN')


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
        except (TooManyTimesException, TimeoutException) as e:
            logger.error('timeout tried times{} {}'.format(times, e))
            self.on_change_proxy(self.opened_url_count)

            self.opened_url_count = 0
            self.check_driver(open_last_page=False)

            return self._get(url, times=times + 1)

    def on_change_proxy(self, opened_times):
        pass
        # db.report_error(
        #     'proxy_opened_urls',
        #     {
        #         'count': opened_times
        #     }
        # )

    def examine_single_apartment(self, apartment):
        try:
            examine_apartment(apartment)
            inserted_id = db.on_pass_validation(apartment)
            self.notify(inserted_id)
            logger.info('pass validation')
        except ValidatorInvalidValueException as e1:
            logger.info('Found invalid value')
            invalid_values = e1.args[1]
            db.report_invalid_value(apartment, invalid_values)
        except Exception as e:
            logger.exception(e)
            db.report_unexpected_error(e, apartment.get(
                'house_url') if apartment else None)

    def notify(self, inserted_id):
        headers = {
            'Authorization': 'Bearer {}'.format(INDIGO_ACCESS_TOKEN)
        }
        res = requests.post('{}/api/v1/subscription/notify'.format(API_DOMAIN),
                            headers=headers,
                            data={
                                'apartment_id': str(inserted_id)
                            }
        )

    def start(self):
        logger.info('START')
        staging_apartments = db.get_unchecked()
        try:
            if not len(staging_apartments):
                raise NoTaskException()
            for apartment in staging_apartments:
                self.examine_single_apartment(apartment)
            logger.info('Task done, sleep for {} min'.format(
                DATA_VALIDATOR_AWAIT_TIME/60))
            sleep(DATA_VALIDATOR_AWAIT_TIME)
            self.start()

        except NoTaskException:
            logger.info('No task to run, sleep for {} min'.format(
                DATA_VALIDATOR_AWAIT_TIME/60))
            sleep(DATA_VALIDATOR_AWAIT_TIME)
            self.start()

        except RecursionError:
            exit(2)
        except Exception as e:
            logger.exception(e)
            sleep(ERROR_AWAIT_TIME)
            self.start()

    def quit(self):
        try:
            self.driver.quit()
        except Exception as e:
            logger.error(f'QUIT ERROR {e}')
