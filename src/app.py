from workers import start_worker
from queues import q_detail_crawler, q_url_crawler, q_validator
import atexit
from db import DB
import os
from multiprocessing import Pool, cpu_count
from jobs import crawl_by_district, crawl_by_metro_station, validate_data, fill_missing_info, enqueue_url_crawler, crawl_detail
from utils.logger import logger
from scheduler import sched


# every day at 9pm
sched.add_job(crawl_by_district, 'cron', hour=14)

# every day at 3am
sched.add_job(crawl_by_metro_station, 'cron', hour=20)

# every 15 minutes
sched.add_job(validate_data, 'interval', minutes=15)

# every 6 hour
sched.add_job(fill_missing_info, 'interval', hours=6)

# every 30 minutes
sched.add_job(crawl_detail, 'interval', minutes=30)

# run now
sched.add_job(crawl_detail, 'date')


@atexit.register
def on_exit():
    sched.shutdown()


def main():
    # enqueue_url_crawler()
    try:
        sched.start()
        cpu_num = max(4, cpu_count())
        p = Pool(cpu_num)
        for i in range(cpu_num):
            if i == 0:
                p.apply_async(start_worker, args=(
                    ['validator', 'url_crawler', 'detail_crawler'],))
            else:
                p.apply_async(start_worker, args=(
                    ['url_crawler', 'detail_crawler'],))
        p.close()
        p.join()
    except Exception as e:
        logger.error('MAIN {}'.format(e))


if __name__ == '__main__':
    main()
