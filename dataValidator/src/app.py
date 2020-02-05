from utils.util import get_root_pth
import sys
sys.path.append(str(get_root_pth()))
from time import sleep
from crawler import DataValidator
from common.utils.logger import logger
import os
import traceback


def start_task():
    # try:
    sleep(3)
    crawler = DataValidator(
        os.environ.get('CITY'),
        os.environ.get('CITY_URL'),
        os.environ.get('SOURCE')
    )
    crawler.start()
        # crawler.start()
    # except Exception as e:
    #     logger.error(e)
    #     logger.error(traceback.format_exc())
    #     sleep(60*5)
    #     start_task()


if __name__ == '__main__':
    start_task()
    # sleep(600)
