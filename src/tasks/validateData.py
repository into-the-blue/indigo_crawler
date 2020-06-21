from selenium.common.exceptions import InvalidSessionIdException, WebDriverException, TimeoutException
from tqdm import tqdm
from time import sleep
from db import mongo
from utils.logger import logger
from utils.constants import ERROR_AWAIT_TIME, DATA_VALIDATOR_AWAIT_TIME
from exceptions import ProxyBlockedException, UrlExistsException, ApartmentExpiredException, NoTaskException, ValidatorInvalidValueException, TooManyTimesException
from random import shuffle
from locateElms import find_apartments_in_list
from .validator import examine_apartment
import traceback
import requests
import os

INDIGO_ACCESS_TOKEN = os.environ.get('INDIGO_ACCESS_TOKEN')
API_DOMAIN = os.environ.get('API_DOMAIN')


class DataValidator(object):
    def __init__(self):
        pass

    def examine_single_apartment(self, apartment):
        try:
            examine_apartment(apartment)
            inserted_id = mongo.on_pass_validation(apartment)
            self.notify(inserted_id)
            logger.info('pass validation')
        except ValidatorInvalidValueException as e1:
            logger.info('Found invalid value')
            invalid_values = e1.args[1]
            mongo.report_invalid_value(apartment, invalid_values)
        except Exception as e:
            logger.exception(e)
            mongo.report_unexpected_error('data_validator', e, apartment.get(
                'house_url') if apartment else None)

    def notify(self, inserted_id):
        headers = {
            'Authorization': 'Bearer {}'.format(INDIGO_ACCESS_TOKEN)
        }
        try:
            res = requests.post('{}/api/v1/subscription/notify'.format(API_DOMAIN),
                                headers=headers,
                                data={
                                    'apartment_id': str(inserted_id)
            }
            )
            mongo.update_apartment(inserted_id, {
                'notification_sent': True
            })
        except Exception:
            mongo.update_apartment(inserted_id, {
                'notification_sent': False
            })

    def validate(self, apartment):
        logger.info('START')
        try:
            self.examine_single_apartment(apartment)
        except Exception as e:
            logger.exception(e)
