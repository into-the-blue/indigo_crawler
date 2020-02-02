from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
from pytz import utc, timezone
from crawler import GrapPage
from utils.util import logger
from db.db import db
from aMap.getCoordinates import getGeoInfo
from time import sleep
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
    jobstores=jobstores, executors=executors, job_defaults=job_defaults, timezone=zh_sh)


def start_by_metro():
    ins = GrapPage('sh', 'https://sh.zu.ke.com/zufang')
    _id = db.start_cron_job('crawl_by_metro')
    try:
        ins.start_by_metro()
        db.update_cron_job(_id)
        ins.quit()
    except Exception as e:
        db.update_cron_job(_id, 'error', str(e))
        logger.error(e)
        ins.quit()
        raise e


def start_by_latest():
    logger.info('Cron job start, job name: start_by_latest')
    ins = GrapPage('sh', 'https://sh.zu.ke.com/zufang')
    _id = db.start_cron_job('crawl_by_latest')
    try:
        ins.start_by_latest()
        db.update_cron_job(_id)
        ins.quit()
    except Exception as e:
        db.update_cron_job(_id, 'error', str(e))
        logger.error(e)
        ins.quit()
        raise e


def start_filling_missing():
    ins = GrapPage('sh', 'https://sh.zu.ke.com/zufang')
    _id = db.start_cron_job('fill_missing_info')
    try:
        ins.start_filling_missing_info()
        db.update_cron_job(_id)
        ins.quit()
    except Exception as e:
        db.update_cron_job(_id, 'error', str(e))
        logger.error(e)
        ins.quit()
        raise e


def _filling_missing_geo_info():
    datas = list(db.findApartmentsWithoutCoor())
    lth = len(datas)
    if(lth > 0):
        for data in datas:
            city = data.get('city')
            district = data.get('district')
            bizcircle = data.get('bizcircle')
            community_name = data.get('community_name')
            result = getGeoInfo(city, district, community_name, bizcircle)
            lng = result['location']['lng']
            lat = result['location']['lat']
            if (result['confidence'] < 60):
                logger.info('Confidence less than 60 {}+{},{}, {}'.format(
                    district, community_name, result['confidence'], data['house_id']
                ))
            db.update_apartment(data.get('house_id'), {
                'lng': lng,
                'lat': lat,
                'geo_info': result
            })
        logger.info(f'{lth}, BATCH DONE')
        return _filling_missing_geo_info()
    else:
        logger.info('FILL GEO INFO DONE')


def filling_missing_geo_info():
    _id = db.start_cron_job('filling_missing_geo_info')
    try:
        _filling_missing_geo_info()
        db.update_cron_job(_id)
    except Exception as e:
        db.update_cron_job(_id, 'error', str(e))
        logger.error(e)


# cron job
# everyday 11:00, 19:00
scheduler.add_job(start_by_latest, trigger='cron', hour='3')
scheduler.add_job(start_by_metro, trigger='cron', hour='11')
scheduler.add_job(start_by_metro, trigger='cron', hour='19')
# everyday 0:00
scheduler.add_job(start_filling_missing, trigger='cron', hour='0')

# everyday 3:00
scheduler.add_job(filling_missing_geo_info, trigger='cron', hour='3')

scheduler._logger = logger

if __name__ == '__main__':
    # sleep(5)
    # start_by_metro()
    scheduler.start()
