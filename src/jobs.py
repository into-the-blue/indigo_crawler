from tasks import DataValidator, DetailCrawler, UrlCrawler
from workers import start_worker
from queues import q_detail_crawler, q_url_crawler, q_validator
from db import DB, redis_conn
from utils.logger import logger
from utils.constants import URL_CRAWLER_TASK_BY_LATEST
from datetime import timedelta
import os
from multiprocessing import Pool
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


def get_crawl_by_latest_await_time():
    hour = datetime.now().hour+8
    if 0 <= hour <= 6:
        return (8-hour)*60

    if 20 <= hour <= 23:
        return (24-hour+6)*60

    return 120

# MAXIMAL_NUMBER_OF_TASKS = 3600*4


BATCH_SIZE_OF_DETAIL_CRAWLER = 1800 * 5

BATCH_SIZE_OF_MISSING_INFO = 50000


def crawl_by_district():
    # num_of_idle = db_ins.get_num_of_idle_tasks()
    # if num_of_idle >= MAXIMAL_NUMBER_OF_TASKS*8:
    #     return
    for city in CITIES:
        job_id = db_ins.on_job_start('crawl_by_district',
                                     city=city.get('city'))
        ins = UrlCrawler()
        ins.setup_city_and_source(city)
        logger.info(
            '[crawl_by_district] [{}] enqueue job'.format(city.get('city')))
        q_url_crawler.enqueue(ins.start_by_district, args=[
                              city.get('city'), job_id], job_timeout='5h')


def crawl_by_metro_station():
    # num_of_idle = db_ins.get_num_of_idle_tasks()
    # if num_of_idle >= MAXIMAL_NUMBER_OF_TASKS*8:
    #     return
    for city in CITIES:
        job_id = db_ins.on_job_start('crawl_by_metro_station',
                                     city=city.get('city'))
        ins = UrlCrawler()
        ins.setup_city_and_source(city)
        logger.info(
            '[crawl_by_metro_station] [{}] enqueue job'.format(city.get('city')))
        q_url_crawler.enqueue(ins.start_by_metro, args=[
                              city.get('city'), job_id], job_timeout='5h')


def on_finish_url_crawling(taskname=URL_CRAWLER_TASK_BY_LATEST, url_count=0, city=None, job_id=None):
    logger.info('[{}] [on_finish_url_crawling] Done {} {}'.format(
        city.get('city'), taskname, url_count))

    next_run_at = datetime.now() + timedelta(minutes=get_crawl_by_latest_await_time())
    if taskname == URL_CRAWLER_TASK_BY_LATEST:
        logger.info(
            '[on_finish_url_crawling] enqueue url crawler, next run at {}'.format(next_run_at))
        q_url_crawler.enqueue_at(
            next_run_at, enqueue_url_crawler, args=(city,))


def enqueue_url_crawler_normal():
    for city in CITIES:
        logger.info(
            '[enqueue_url_crawler] [{}] enqueue job normal'.format(city.get('city')))
        ins = UrlCrawler()
        ins.setup_city_and_source(city)
        q_url_crawler.enqueue(ins.start_by_url,
                              args=(city.get('url'),),
                              kwargs={'by_latest': False},
                              job_timeout='2h')


def enqueue_url_crawler(_city=None):
    # num_of_idle = db_ins.get_num_of_idle_tasks()
    # if num_of_idle >= MAXIMAL_NUMBER_OF_TASKS:
    #     logger.warning(
    #         'Too many tasks: {}'.format(num_of_idle))
    #     delayed = math.floor(num_of_idle/3600/4)
    #     q_url_crawler.enqueue_at(
    #         datetime.now()+timedelta(minutes=delayed*60), enqueue_url_crawler, args=(_city,))
    #     logger.warning('enqueued, execute after {}h'.format(delayed))
    #     return
    _cities = CITIES
    if _city:
        _cities = [_city]
    for city in _cities:
        job_id = db_ins.on_job_start(URL_CRAWLER_TASK_BY_LATEST,
                                     city=city.get('city'))
        logger.info(
            '[enqueue_url_crawler] [{}] enqueue job'.format(city.get('city')))
        ins = UrlCrawler(on_finish_url_crawling)
        ins.setup_city_and_source(city)
        q_url_crawler.enqueue(ins.start_by_url,
                              args=(city.get('url'),),
                              kwargs={'job_id': job_id},
                              job_timeout='2h')


def validate_data():
    staging_apts = db_ins.get_unchecked_staging_apts()
    if not len(staging_apts):
        db_ins.on_job_start('validate_data', message='no task')
        return
    ins = DataValidator()
    job_ids = q_validator.job_ids
    enqueued_job_num = 0
    logger.info('[validate_data] total: {}, existing: {}'.format(
        len(staging_apts), len(job_ids)))
    for apt in staging_apts:
        if len(job_ids) == 0 or apt.get('house_code') not in job_ids:
            enqueued_job_num += 1
            q_validator.enqueue(ins.examine_single_apartment, args=[
                                apt], job_id=apt.get('house_code'))
    db_ins.on_job_start('validate_data', count=enqueued_job_num)
    logger.info('[validate_data] total: {}, enqueued: {}'.format(
        len(staging_apts), enqueued_job_num))


def crawl_detail():
    logger.info('[crawl_detail] start')
    job_ids = q_detail_crawler.job_ids
    if len(job_ids) >= BATCH_SIZE_OF_DETAIL_CRAWLER:
        db_ins.on_job_start('crawl_detail',
                            message='too many tasks: {}'.format(len(job_ids)))
        return
    tasks = db_ins.find_idle_tasks(BATCH_SIZE_OF_DETAIL_CRAWLER, job_ids)
    if not len(tasks):
        logger.info('[crawl_detail] no task available')
        db_ins.on_job_start('crawl_detail', message='no task')
        return
    enqueued_job_num = 0
    logger.info('[crawl_detail] total: {}, existing: {}'.format(
        len(tasks), len(job_ids)))
    for task in tasks:
        if len(job_ids) == 0 or task.get('url') not in job_ids:
            enqueued_job_num += 1
            ins = DetailCrawler()
            job = q_detail_crawler.enqueue(ins.start_one_url, args=[
                                           task], job_id=task.get('url'))
    db_ins.on_job_start('crawl_detail',
                        count=enqueued_job_num
                        )
    logger.info('[crawl_detail] total: {}, enqueued: {}'.format(
        len(tasks), enqueued_job_num))


def fill_missing_info():
    job_ids = q_detail_crawler.job_ids
    if len(job_ids) >= BATCH_SIZE_OF_MISSING_INFO:
        db_ins.on_job_start('fill_missing_info',
                            message='too many tasks: {}'.format(len(job_ids))
                            )
        return
    apts = db_ins.get_staging_apts_with_missing_info(
        BATCH_SIZE_OF_MISSING_INFO, job_ids)
    if not len(apts):
        db_ins.on_job_start('fill_missing_info', message='no task')
        return
    enqueued_job_num = 0
    logger.info('[fill_missing_info] total: {}, existing: {}'.format(
        len(apts), len(job_ids)))
    for apt in apts:
        if len(job_ids) == 0 or apt.get('house_code') not in job_ids:
            enqueued_job_num += 1
            ins = DetailCrawler()
            q_detail_crawler.enqueue(ins.start_fill_missing, args=[
                                     apt], job_timeout='20m', job_id=apt.get('house_code'))
    db_ins.on_job_start('fill_missing_info',
                        count=enqueued_job_num
                        )
    logger.info('[fill_missing_info] total: {}, enqueued: {}'.format(
        len(apts), enqueued_job_num))
