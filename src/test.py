from selenium import webdriver
import pandas as pd
from tqdm import tqdm
import time
import json
import codecs
from collections import OrderedDict
import pymongo
import re
import os
from crawlSingleUrl import get_info_of_single_url
from proxy_pool import get_driver_with_proxy
from db import DB
from helper import _print
is_ubuntu = os.environ.get('PY_ENV', 'mac') == 'ubuntu'


def test():
    # if(is_ubuntu):
    #     print('ubuntu')
    #     display = Display(visible=0)
    #     display.start()
    _print('start')
    driver = get_driver_with_proxy()
    driver.get('http://baidu.com')
    _print(driver.page_source)


if __name__ == '__main__':
    test()
