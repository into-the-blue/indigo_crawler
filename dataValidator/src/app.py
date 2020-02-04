from utils.util import get_root_pth
import sys
import os
sys.path.append(str(get_root_pth()))
from common.utils.logger import logger
from crawler import UrlCrawler
from time import sleep


def start_task():
    try:
        sleep(3)
        crawler = UrlCrawler(
            os.environ.get('CITY'),
            os.environ.get('CITY_URL'),
            os.environ.get('SOURCE')
        )
        crawler.start()
        sleep(10)
        # crawler.start()
    except Exception as e:
        logger.error(e)
        sleep(60)


if __name__ == '__main__':
    start_task()
    # sleep(600)