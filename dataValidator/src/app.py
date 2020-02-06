from utils.util import get_root_pth
import sys
sys.path.append(str(get_root_pth()))
from time import sleep
from crawler import DataValidator
from common.utils.logger import logger
import os
import traceback


def start_task():
    sleep(3)
    crawler = DataValidator(
        os.environ.get('CITY'),
        os.environ.get('CITY_URL'),
        os.environ.get('SOURCE')
    )
    crawler.start()


if __name__ == '__main__':
    start_task()
