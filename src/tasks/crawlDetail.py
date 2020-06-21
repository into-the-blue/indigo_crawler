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
from .crawlSingleUrl import get_info_of_single_url
from browser import BaseWebDriver


class DetailCrawler(BaseWebDriver):
    def __init__(self, ):
        super().__init__()

    def start_one_url(self, task):
        try:
            logger.info('Start crawl new link')
            self.get(task.get('url'))
            logger.info('Url opened')
            info = get_info_of_single_url(self.driver, task.get('url'))
            logger.info('Data get')
            mongo.insert_into_staing(task, info)
        except ApartmentExpiredException:
            logger.info('Url expired')
            mongo.task_expired(task)
        except NoSuchElementException:
            # probably proxy blocked
            logger.info('Elm not found')
            self.renew_driver()
        except (TimeoutException, WebDriverException, InvalidSessionIdException):
            logger.info('Session timeout')
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
            logger.info('Start fill in missing info')
            self.get(apartment.get('house_url'))
            logger.info('Url opened')
            info = get_info_of_single_url(
                self.driver, apartment.get('house_url'))
            logger.info('Data get')
            mongo.update_missing_info(apartment, info)
            sleep(2)
        except ApartmentExpiredException:
            logger.info('Url expired')
            mongo.update_missing_info(apartment, {
                'expired': True,
            })
        except NoSuchElementException:
            logger.info('Elm not found')
        except (TimeoutException, WebDriverException, InvalidSessionIdException):
            logger.info('Session timeout')
            self.renew_driver()
        except (TooManyTimesException):
            pass
        except Exception as e:
            logger.exception(e)
        finally:
            self.quit()
