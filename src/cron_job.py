from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
from pytz import utc,timezone
from crawler import GrapPage
from utils.util import _error, logger
from db import db

zh_sh = timezone('Asia/Shanghai')

jobstores = {
    'default': MemoryJobStore()
}

executors = {
    'default': ThreadPoolExecutor(20),
    'processpool': ProcessPoolExecutor(10)
}

job_defaults = {
    'coalesce': False,
    'max_instances': 3
}

scheduler = BlockingScheduler(
    jobstores=jobstores, executors=executors, job_defaults=job_defaults,timezone=zh_sh)


def start_by_metro():
    ins = GrapPage('sh','https://sh.zu.ke.com/zufang')
    _id = db.start_cron_job('crawl_by_metro')
    try:
        ins.start_by_metro()
        db.update_cron_job(_id)
    except Exception as e:
        db.update_cron_job(_id,'error',str(e))
        _error(e)
    finally:
        ins.quit()

def start_by_latest():
    ins = GrapPage('sh','https://sh.zu.ke.com/zufang')
    _id = db.start_cron_job('crawl_by_latest')
    try:
        ins.start_by_latest()
        db.update_cron_job(_id)
    except Exception as e:
        db.update_cron_job(_id,'error',str(e))
        _error(e)
    finally:
        ins.quit()

def start_filling_missing():
    ins = GrapPage('sh','https://sh.zu.ke.com/zufang')
    _id = db.start_cron_job('fill_missing_info')
    try:
        ins.start_filling_missing_info()
        db.update_cron_job(_id)
    except Exception as e:
        db.update_cron_job(_id,'error',str(e))
        _error(e)
    finally:
        ins.quit()

# everyday 8:00, 18:00
scheduler.add_job(start_by_metro,trigger='cron',hour='8,18')

# everyday 0:00
scheduler.add_job(start_filling_missing,trigger='cron',hour='0')

scheduler._logger = logger

if __name__ == '__main__':
    scheduler.start()