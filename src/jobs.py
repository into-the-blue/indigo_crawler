from tasks import DataValidator, DetailCrawler, UrlCrawler
from workers import start_worker
from queues import q_detail_crawler, q_url_crawler, q_validator
from db import DB
from utils.logger import logger
from utils.constants import URL_CRAWLER_TASK_BY_LATEST
from datetime import timedelta
import os
from multiprocessing import Pool
from scheduler import sched
from datetime import datetime
from rq.job import Job

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
        q_url_crawler.enqueue(ins.start_by_district, args=[
                              city.get('city')], timeout='5h')
        logger.info('crawl by districtjobs enqueued')


def crawl_by_metro_station():
    for city in CITIES:
        ins = UrlCrawler()
        ins.setup_city_and_source(city.get('city'), city.get('source'))
        q_url_crawler.enqueue(ins.start_by_metro, args=[
                              city.get('city')], timeout='5h')
        logger.info('crawl by metro jobs enqueued')


def validate_data():
    staging_apts = db_ins.get_unchecked_staging_apts()
    if not len(staging_apts):
        return
    ins = DataValidator()
    job_ids = q_validator.job_ids
    for apt in staging_apts:
        if apt.get('house_code') not in job_ids:
            job = Job.create(ins.examine_single_apartment, args=[
                             apt], id=apt.get('house_code'))
            q_validator.enqueue_job(job)


def crawl_detail():
    tasks = db_ins.find_idle_tasks()
    if not len(tasks):
        return
    job_ids = q_detail_crawler.job_ids
    for task in tasks:
        if task.get('url') not in job_ids:
            ins = DetailCrawler()
            job = Job.create(ins.start_by_url, args=[task], id=task.get('url'))
            q_detail_crawler.enqueue_job(job)


def fill_missing_info():
    apts = db_ins.get_missing_info()
    if not len(apts):
        return
    job_ids = q_detail_crawler.job_ids
    for apt in apts:
        if apt.get('house_code') not in job_ids:
            ins = DetailCrawler()
            job = Job.create(ins.start_fill_missing, args=[
                             apt], id=apt.get('house_code'))
            q_detail_crawler.enqueue_job(job)


def on_finish_url_crawling(taskname, url_count):
    print('[UrlCrawler] Done', taskname, url_count)
    if url_count > 0:
        crawl_detail()
    if taskname == URL_CRAWLER_TASK_BY_LATEST:
        # if url_count <= 50:
        #     sched.add_job(enqueue_url_crawler, 'date',
        #                   run_date=datetime.now() + timedelta(minutes=180))
        #     return
        # if url_count <= 100:
        #     sched.add_job(enqueue_url_crawler, 'date',
        #                   run_date=datetime.now() + timedelta(minutes=45))
        #     return
        # if url_count <= 200:
        #     sched.add_job(enqueue_url_crawler, 'date',
        #                   run_date=datetime.now() + timedelta(minutes=30))
        #     return

        sched.add_job(enqueue_url_crawler, 'date',
                      run_date=datetime.now() + timedelta(minutes=180))


def enqueue_url_crawler():
    for city in CITIES:
        ins = UrlCrawler(on_finish_url_crawling)
        ins.setup_city_and_source(city.get('city'), city.get('source'))
        q_url_crawler.enqueue(ins.start_by_url, args=[
                              city.get('url')], timeout='1h')
