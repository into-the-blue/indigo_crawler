import re
import sys
import logging

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
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
