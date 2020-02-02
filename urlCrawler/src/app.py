from utils.util import get_root_pth
import sys
sys.path.append(str(get_root_pth()))
from common.utils.logger import logger
from crawler import UrlCrawler
from time import sleep


def start_task():
    try:
        sleep(10)
        crawler = UrlCrawler(
            'shanghai',
            'https://sh.zu.ke.com/zufang',
            'beike'
        )
        crawler.start_by_metro()
        sleep(10)
        crawler.start()
    except Exception as e:
        logger.error(e)
        sleep(60)


if __name__ == '__main__':
    start_task()
    # sleep(600)