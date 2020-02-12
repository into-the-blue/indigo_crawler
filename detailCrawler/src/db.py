from common.db import DB
from datetime import datetime, timedelta
from common.utils.logger import logger
from common.aMap.getCoordinates import get_location_info_from_apartment_info


class MyDB(DB):
    def __init__(self):
        super().__init__()

    def get_one_task(self):
        res = self.tasks.find_one({
            '$query': {
                'status': 'idle',
                'failed_times': {'$lt': 3}
            },
            '$orderby': {
                'updated_at': 1
            }
        })
        if res:
            self.tasks.update_one(
                {'_id': res.get('_id'), },
                {'$set': {
                    'status': 'processing',
                    'updated_at': datetime.now()
                }}
            )
        return res

    def update_failure(self, task,  err=None, page_source=None):
        '''
        if failed,
        increase `failed_times`,
        if `failed_times` greater than 3
        set status to `error`
        save `page_source`
        '''
        toupdate = {
            'failed_times': task.get('failed_times')+1,
            'status': 'idle'
        }
        if toupdate['failed_times'] >= 3:
            toupdate['status'] = 'error'
            if page_source:
                page_source_id = self.insert_page_source(
                    task.get('url'), page_source)
                toupdate['page_source_id'] = page_source_id

        self.tasks.update_one(
            {'_id': task.get('_id')},
            {'$set': {**toupdate, 'updated_at': datetime.now()}}
        )
        
        if err:
            self.report_unexpected_error(err, task.get('url'))

    def get_missing_info(self):
        res = self.apartments_staging.find_one({
            'missing_info': True, 'updated_time': {'$lte': datetime.now()-timedelta(hours=8)},
            '$or': [
                {'failed_times': {'$exists': False}},
                {'failed_times': {'$lt': 1}},
                {'check_times': {'$exists': False}},
                {'check_times': {'$lt': 3}}
            ],
        })

        if res:
            self.apartments_staging.update_one(
                {'_id': res.get('_id')},
                {
                    '$set': {
                        'updated_time': datetime.now(),
                        'checked_times': res.get('checked_times', 0)+1
                    }
                }
            )
        return res

    def update_missing_info(self, apartment, updated):
        self.apartments_staging.update_one(
            {'_id': apartment.get('_id')},
            {'$set': {
                **updated,
                'updated_time': datetime.now()
            }}
        )

    def report_error(self, message, url, payload):
        return super()._report_error({
            'error_source': 'detail_crawler',
            'url': url,
            'message': message,
            'payload': payload
        })

    def report_unexpected_error(self, *args, **kwargs):
        return super().report_unexpected_error('detail_crawler', *args, **kwargs)

    def task_expired(self, task):
        self.tasks.update_one(
            {'_id': task.get('_id')},
            {'$set': {'status': 'expired', 'updated_at': datetime.now()}}
        )

    def insert_into_staing(self, task, apartment_doc):
        '''
        insert apartment info into staging
        set task status to done
        '''
        self.tasks.update_one(
            {'_id': task.get('_id')},
            {'$set': {'status': 'done', 'updated_at': datetime.now()}}
        )
        apartment = {
            **apartment_doc,
            'created_time': datetime.now(),
            'updated_time': datetime.now(),
        }
        location_info = get_location_info_from_apartment_info(apartment_doc)

        apartment = {
            **apartment,
            **location_info
        }

        station_info = task.get('station_info')
        if station_info:
            line_ids = station_info.get('line_ids')
            station_id = station_info.get('station_id')
            apartment = {
                **apartment,
                'station_ids': [station_id],
                'line_ids': line_ids,
            }

        self.apartments_staging.insert_one(apartment)


db = MyDB()
