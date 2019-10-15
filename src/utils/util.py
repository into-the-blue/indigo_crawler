import re
import sys
import logging
from datetime import datetime, timezone, timedelta

logging.basicConfig(stream=sys.stdout, level=logging.INFO,
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger('INDIGO')


def extract_house_id(house_code):
    return re.findall('[0-9]+', house_code)[0]


def extract_house_code_from_url(url):
    return re.findall('(?<=\/)[a-zA-Z]+[0-9]+(?=\.)', url)[0]


def normal_msg(args):
    return ' '.join(map(lambda x: str(x), args))


def _print(*args):
    logger.info(normal_msg(args))


def _error(*args):
    logger.error(normal_msg(args))


def currentDate():
    tz = timezone(timedelta(hours=8))
    return datetime.now(tz=tz).strftime('%Y-%m-%d %H:%M:%S')