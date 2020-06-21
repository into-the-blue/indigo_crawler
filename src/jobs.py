from tasks import DataValidator, DetailCrawler, UrlCrawler
from workers import start_worker
from queues import q_detail_crawler, q_url_crawler, q_validator
from db import DB
from utils.logger import logger
from utils.constants import URL_CRAWLER_TASK_BY_LATEST
from datetime import timedelta
import os
from multiprocessing import Pool

CITIES = [
    {
        'city': 'shanghai',
        'source': 'beike',
        'url': 'https://sh.zu.ke.com/zufang'
    },
    {
        'city': 'beijing',
        'source': 'beike',
        'url': 'https://bj.zu.ke.com/zufang'
    },
    {
        'city': 'guangzhou',
        'source': 'beike',
        'url': 'https://gz.zu.ke.com/zufang'
    },
    {
        'city': 'hangzhou',
        'source': 'beike',
        'url': 'https://hz.zu.ke.com/zufang'
    },
    {
        'city': 'shenzhen',
        'source': 'beike',
        'url': 'https://sz.zu.ke.com/zufang'
    },
    {
        'city': 'nanjing',
        'source': 'beike',
        'url': 'https://nj.zu.ke.com/zufang'
    },
    {
        'city': 'suzhou',
        'source': 'beike',
        'url': 'https://su.zu.ke.com/zufang'
    },
]

db_ins = DB()
def crawl_by_district():
    for city in CITIES:
        ins = UrlCrawler()
        ins.setup_city_and_source(city.get('city'), city.get('source'))
        q_url_crawler.enqueue(ins.start_by_district, args=[city.get('city')])
        logger.info('crawl by districtjobs enqueued')


def crawl_by_metro_station():
    for city in CITIES:
        ins = UrlCrawler()
        ins.setup_city_and_source(city.get('city'), city.get('source'))
        q_url_crawler.enqueue(ins.start_by_metro, args=[city.get('city')])
        logger.info('crawl by metro jobs enqueued')


def validate_data():
    ins = DataValidator()
    staging_apts = db_ins.get_unchecked_staging_apts()
    for apt in staging_apts:
        q_validator.enqueue(ins.examine_single_apartment, args=[apt])


def crawl_detail():
    tasks = db_ins.find_idle_tasks()
    if not len(tasks):
        return
    ins = DetailCrawler()
    for task in tasks:
        q_detail_crawler.enqueue(ins.start_one_url, args=[task])


def fill_missing_info():
    apts = db_ins.get_missing_info()
    if not len(apts):
        return
    ins = DetailCrawler()
    for task in apts:
        q_detail_crawler.enqueue(ins.start_one_url, args=[task])


def on_finish_url_crawling(taskname, url_count):
    print('on_finish_url_crawling', taskname, url_count)
    if url_count > 0:
        crawl_detail()
    if taskname == URL_CRAWLER_TASK_BY_LATEST:
        if url_count <= 50:
            q_url_crawler.enqueue_in(
                timedelta(minutes=60), enqueue_url_crawler)
            return
        if url_count <= 100:
            q_url_crawler.enqueue_in(
                timedelta(minutes=45), enqueue_url_crawler)
            return
        if url_count <= 200:
            q_url_crawler.enqueue_in(
                timedelta(minutes=30), enqueue_url_crawler)
            return

        q_url_crawler.enqueue_in(timedelta(minutes=60), enqueue_url_crawler)


def enqueue_url_crawler():
    for city in CITIES:
        ins = UrlCrawler(on_finish_url_crawling)
        ins.setup_city_and_source(city.get('city'), city.get('source'))
        q_url_crawler.enqueue(ins.start_by_url, args=[city.get('url')])