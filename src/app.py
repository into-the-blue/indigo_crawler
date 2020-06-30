from workers import start_worker
from queues import q_detail_crawler, q_url_crawler, q_validator
import atexit
from db import DB
import os
from multiprocessing import Pool, cpu_count
from jobs import crawl_by_district, crawl_by_metro_station, validate_data, fill_missing_info, enqueue_url_crawler, crawl_detail
from utils.logger import logger
from utils.constants import SCOPE, ROLE
from scheduler import sched
from time import sleep
import math

IS_MASTER = ROLE == 'master'

if IS_MASTER:
    q_url_crawler.empty()
    q_validator.empty()
    q_detail_crawler.empty()
SCOPES = ['detail_crawler',
          'url_crawler'] if SCOPE == '*' else SCOPE.split('|')


def schedule_validator():
    # every 15 minutes
    sched.add_job(validate_data, 'interval', minutes=15)


def schedule_crawler_detail_jobs():
    # every 6 hour
    sched.add_job(fill_missing_info, 'interval', hours=6)

    # every 30 minutes
    sched.add_job(crawl_detail, 'interval', minutes=30)

    # run now
    sched.add_job(crawl_detail, 'date')


def schedule_url_crawler():
    # every day at 9pm
    sched.add_job(crawl_by_district, 'cron', hour=14)

    # every day at 3am
    sched.add_job(crawl_by_metro_station, 'cron', hour=20)


def start_schedule():
    if IS_MASTER:
        schedule_crawler_detail_jobs()
        schedule_validator()
        schedule_url_crawler()
        sched.start()
        enqueue_url_crawler()


@atexit.register
def on_exit():
    sched.shutdown()


def main():
    try:
        # wait for webdriver up
        sleep(20)
        start_schedule()
        # cpu_num = math.floor(cpu_count()*1.5)
        cpu_num = 1
        p = Pool(cpu_num)
        logger.info('cpu num {}'.format(cpu_num))
        for i in range(cpu_num):
            if i == 0:
                _scopes = SCOPES
                _scopes.reverse()
                if IS_MASTER and 'validator' not in _scopes:
                    _scopes = ['validator', *_scopes]
                p.apply_async(start_worker, args=(
                    _scopes,))
            elif i < 3:
                p.apply_async(start_worker, args=(SCOPES,))
            else:
                p.apply_async(start_worker, args=(
                    ['detail_crawler'],))
        p.close()
        p.join()
    except Exception as e:
        logger.error('MAIN {}'.format(e))


if __name__ == '__main__':
    main()
