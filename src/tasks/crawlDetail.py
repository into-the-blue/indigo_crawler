from selenium.common.exceptions import InvalidSessionIdException, WebDriverException, TimeoutException, NoSuchElementException
from tqdm import tqdm
from time import sleep
from db import mongo
from utils.logger import logger
from utils.util import safely_get_url_from_driver
from utils.constants import DETAIL_CRAWLER_AWAIT_TIME, ERROR_AWAIT_TIME, TASK_DONE_AWAIT_TIME
from exceptions import ProxyBlockedException, UrlExistsException, ApartmentExpiredException, NoTaskException, TooManyTimesException
from random import shuffle
from locateElms import find_apartments_in_list
from .crawlDetailOfSingleUrl import get_info_of_single_url
from browser import BaseWebDriver
import os


# cn-sh-1 / fc-sg-1
SERVER_NAME = os.getenv('SERVER_NAME', 'unknown')


class DetailCrawler(BaseWebDriver):
    def __init__(self, ):
        super().__init__()
        if 'cn' not in SERVER_NAME.lower():
            self.setup_proxy_when_init_driver = True

    def __del__(self):
        class_name = self.__class__.__name__
        self.quit()
        # logger.info('[{}] detroyed'.format(class_name))

    def start_one_url(self, task):
        try:
            logger.info(
                '[{}] [DetailCrawler] Start crawl new link'.format(task.get('city')))
            self.get(task.get('url'))
            logger.info(
                '[{}] [DetailCrawler] Url opened'.format(task.get('city')))
            info = get_info_of_single_url(self.driver, task.get('url'))
            logger.info(
                '[{}] [DetailCrawler] Data get'.format(task.get('city')))
            mongo.insert_into_staing(task, info)
        except ApartmentExpiredException:
            logger.info(
                '[{}] [DetailCrawler] Url expired'.format(task.get('city')))
            mongo.task_expired(task)
        except NoSuchElementException:
            # probably proxy blocked
            logger.info(
                '[{}] [DetailCrawler] Elm not found'.format(task.get('city')))
            self.renew_driver()
        except (TimeoutException, WebDriverException, InvalidSessionIdException):
            logger.info(
                '[{}] [DetailCrawler] Session timeout'.format(task.get('city')))
            self.renew_driver()
        except (TooManyTimesException):
            pass
        except Exception as e:
            logger.exception(e)
            mongo.update_failure_task(task, e, self.driver.page_source)
        finally:
            self.quit()

    def start_fill_missing(self, apartment):
        '''
        fill in missing info
        '''
        try:
            logger.info('[{}] [DetailCrawler] Start fill in missing info'.format(
                apartment.get('city')))
            self.get(apartment.get('house_url'))
            logger.info('[{}] [DetailCrawler] Url opened'.format(
                apartment.get('city')))
            info = get_info_of_single_url(
                self.driver, apartment.get('house_url'))
            logger.info('[{}] [DetailCrawler] Data get'.format(
                apartment.get('city')))
            mongo.update_missing_info(apartment, info)
            sleep(2)
        except ApartmentExpiredException:
            logger.info('[{}] [DetailCrawler] Url expired'.format(
                apartment.get('city')))
            mongo.update_missing_info(apartment, {
                'expired': True,
            })
        except NoSuchElementException:
            logger.info('[{}] [DetailCrawler] Elm not found'.format(
                apartment.get('city')))
        except (TimeoutException, WebDriverException, InvalidSessionIdException):
            logger.info('[{}] [DetailCrawler] Session timeout'.format(
                apartment.get('city')))
            self.renew_driver()
        except (TooManyTimesException):
            pass
        except Exception as e:
            logger.error('[{}] [DetailCrawler] [start_fill_missing] err'.format(
                apartment.get('city')))
            logger.exception(e)
        finally:
            self.quit()
