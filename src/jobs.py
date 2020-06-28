from tasks import DataValidator, DetailCrawler, UrlCrawler
from workers import start_worker
from queues import q_detail_crawler, q_url_crawler, q_validator
from db import DB, redis_conn
from utils.logger import logger
from utils.constants import URL_CRAWLER_TASK_BY_LATEST
from datetime import timedelta
import os
from multiprocessing import Pool
from scheduler import sched
from datetime import datetime
from rq.job import Job
from time import sleep
from tqdm import tqdm
import math

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


MAXIMAL_NUMBER_OF_TASKS = 3600*4



def crawl_by_district():
    num_of_idle = db_ins.get_num_of_idle_tasks()
    if num_of_idle >= MAXIMAL_NUMBER_OF_TASKS:
        return
    for city in CITIES:
        ins = UrlCrawler()
        ins.setup_city_and_source(city)
        logger.info(
            '[crawl_by_district] [{}] enqueue job'.format(city.get('city')))
        q_url_crawler.enqueue(ins.start_by_district, args=[
                              city.get('city')], job_timeout='5h')


def crawl_by_metro_station():
    num_of_idle = db_ins.get_num_of_idle_tasks()
    if num_of_idle >= MAXIMAL_NUMBER_OF_TASKS:
        return
    for city in CITIES:
        ins = UrlCrawler()
        ins.setup_city_and_source(city)
        logger.info(
            '[crawl_by_metro_station] [{}] enqueue job'.format(city.get('city')))
        q_url_crawler.enqueue(ins.start_by_metro, args=[
                              city.get('city')], job_timeout='5h')


def on_finish_url_crawling(taskname=URL_CRAWLER_TASK_BY_LATEST, url_count=0, city=None):
    logger.info('[{}] [on_finish_url_crawling] Done {} {}'.format(
        city.get('city'), taskname, url_count))
    if taskname == URL_CRAWLER_TASK_BY_LATEST:
        logger.info('[on_finish_url_crawling] enqueue url crawler')
        q_url_crawler.enqueue_at(
            datetime.now() + timedelta(minutes=180), enqueue_url_crawler, args=(city,))


def enqueue_url_crawler(_city=None):
    num_of_idle = db_ins.get_num_of_idle_tasks()
    if num_of_idle >= MAXIMAL_NUMBER_OF_TASKS:
        logger.warning(
            'Too many tasks: {}'.format(num_of_idle))
        delayed = math.floor(num_of_idle/3600/4)
        q_url_crawler.enqueue_at(
            datetime.now()+timedelta(minutes=delayed*60), enqueue_url_crawler, args=(_city,))
        logger.warning('enqueued, execute after {}h'.format(delayed))
        return
    _cities = CITIES
    if _city:
        _cities = [_city]
    for city in _cities:
        logger.info(
            '[enqueue_url_crawler] [{}] enqueue job'.format(city.get('city')))
        ins = UrlCrawler(on_finish_url_crawling)
        ins.setup_city_and_source(city)
        q_url_crawler.enqueue(ins.start_by_url, args=(
                              city.get('url'),), job_timeout='1h')


def validate_data():
    staging_apts = db_ins.get_unchecked_staging_apts()
    if not len(staging_apts):
        return
    ins = DataValidator()
    job_ids = q_validator.job_ids
    enqueued_job_num = 0
    logger.info('[validate_data] total: {}, existing: {}'.format(
        len(staging_apts), len(job_ids)))
    if len(staging_apts) == len(job_ids):
        return
    for apt in staging_apts:
        if len(job_ids) == 0 or apt.get('house_code') not in job_ids:
            enqueued_job_num += 1
            q_validator.enqueue(ins.examine_single_apartment, args=[
                                apt], job_id=apt.get('house_code'))
    logger.info('[validate_data] total: {}, enqueued: {}'.format(
        len(staging_apts), enqueued_job_num))


def crawl_detail():
    logger.info('[crawl_detail] start')
    job_ids = q_detail_crawler.job_ids
    if len(job_ids) >= 1000:
        return
    tasks = db_ins.find_idle_tasks(1000, job_ids)
    if not len(tasks):
        logger.info('[crawl_detail] no task available')
        return
    enqueued_job_num = 0
    logger.info('[crawl_detail] total: {}, existing: {}'.format(
        len(tasks), len(job_ids)))
    if len(tasks) == len(job_ids):
        return
    for task in tasks:
        if len(job_ids) == 0 or task.get('url') not in job_ids:
            enqueued_job_num += 1
            ins = DetailCrawler()
            job = q_detail_crawler.enqueue(ins.start_one_url, args=[
                                           task], job_id=task.get('url'))
    logger.info('[crawl_detail] total: {}, enqueued: {}'.format(
        len(tasks), enqueued_job_num))


def fill_missing_info():
    job_ids = q_detail_crawler.job_ids
    if len(job_ids) >= 1000:
        return
    apts = db_ins.get_staging_apts_with_missing_info(1000, job_ids)
    if not len(apts):
        return
    enqueued_job_num = 0
    logger.info('[fill_missing_info] total: {}, existing: {}'.format(
        len(apts), len(job_ids)))
    if len(apts) == len(job_ids):
        return
    for apt in apts:
        if len(job_ids) == 0 or apt.get('house_code') not in job_ids:
            enqueued_job_num += 1
            ins = DetailCrawler()
            q_detail_crawler.enqueue(ins.start_fill_missing, args=[
                                     apt], job_timeout='10m', job_id=apt.get('house_code'))
    logger.info('[fill_missing_info] total: {}, enqueued: {}'.format(
        len(apts), enqueued_job_num))
