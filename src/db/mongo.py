import pymongo
import os
from pymongo import IndexModel, ASCENDING, DESCENDING, GEOSPHERE
from datetime import datetime, timedelta
import traceback
from exceptions import ErrorExistsException
from utils.aMap import get_location_info_from_apartment_info

db_username = os.getenv('DB_USERNAME')
db_password = os.getenv('DB_PASSWORD')
db_host = os.getenv('DB_HOST')
db_database = os.getenv('DB_DATABASE')


def rm_useless_values(apartment):
    keys_to_rm = [
        '_id',
        'failed_times'
    ]
    for key in keys_to_rm:
        try:
            del apartment[key]
        except:
            pass
    return apartment


def ensure_indexes_of_apartment(apartments):
    indexes = [
        ('house_code', ASCENDING),
        ('created_time', DESCENDING),
        ('coordinates', GEOSPHERE),
        ('price', ASCENDING),
        ('price_per_square_meter', DESCENDING),
        ('area', ASCENDING),
        ('created_at', DESCENDING)
    ]
    indexes = [IndexModel([i]) for i in indexes]
    apartments.create_indexes(indexes)


def ensure_indexes_of_station(station):
    indexes = [
        ('coordinates', GEOSPHERE),
    ]
    indexes = [IndexModel([i]) for i in indexes]
    station.create_indexes(indexes)


class DB(object):
    def __init__(self):
        self.initDb()

    def initDb(self):
        self.conn = pymongo.MongoClient(
            db_host+':27017', username=db_username, password=db_password, connect=False)
        self.indigo = self.conn[db_database]

        self.line_col = self.indigo.lines
        self.station_col = self.indigo.stations
        self.apartments = self.indigo.apartments

        self.bizcircles_col = self.indigo.bizcircles
        self.errors = self.indigo.errors
        # tasks
        # {
        #   url     string unique
        #   source  beike
        #   city    shanghai | string
        #   status   idle | processing | done | expired
        #   failed_times number
        #   page_source: string
        #   station_info: object
        #   created_at
        #   updated_at
        # }
        self.tasks = self.indigo.tasks
        self.apartments_staging = self.indigo.apartmentsStaging
        self.page_source = self.indigo.pageSource

        self.crawler_col = self.indigo.crawler

        self.agenda_jobs_col = self.indigo.agendaJobs
        # ensure_indexes_of_station(self.station_col)
        # ensure_indexes_of_apartment(self.apartments)
        # ensure_indexes_of_apartment(self.apartments_staging)

    def on_job_start(self, job_name, **kwargs):
        return self.crawler_col.insert_one({
            'name': job_name,
            'phase': 'start',
            'start_at': datetime.now(),
            **kwargs
        }).inserted_id

    def on_job_done(self, job_id, **kwargs):
        if not job_id:
            return

        self.crawler_col.update_one({
            '_id': job_id,
        }, {
            '$set': {
                **kwargs,
                'phase': 'done',
                'finish_at': datetime.now()
            }
        })

    def save_crawler_procedures(self, doc):
        self.crawler_col.insert_one({
            **doc,
            'created_at': datetime.now()
        })

    def get_num_of_idle_tasks(self):
        '''
        get number of idle tasks
        '''
        cursor = self.tasks.find({'status': 'idle'})
        return cursor.count()

    def find_all_stations(self, city):
        '''
            get all metro stations of a city
        '''
        return list(self.station_col.find({
            '$query': {'city': city},
            '$orderby': {'priority': -1}
        }))

    def find_all_bizcircles(self, city):
        '''
        get all bizcircles of a city
        '''
        return list(self.bizcircles_col.find({
            '$query': {'city': city},
            '$orderby': {'priority': -1}
        }))

    def find_idle_tasks(self, limit=1000, exclude=[]):
        '''
        get idles tasks

        args:
            limit:
                the number of tasks to return
            exclude: a list of urls
                exclude urls in list
        '''
        tasks = self.tasks.find({
            'status': 'idle',
            'failed_times': {'$lt': 3},
            'url': {'$nin': exclude}
        }).limit(limit)
        return list(tasks)

    def update_apartment(self, apt_id, doc):
        '''
        update a apartment
        args:
            apt_id: object id
                id
            doc:
                document to update
        '''
        self.apartments.update_one(
            {'_id': apt_id},
            {'$set': {
                **doc,
                'updated_time': datetime.now()
            }}
        )

    def update_priority_of_station(self, station_id, priority):
        '''
        update priority of station
        '''
        self.station_col.update_one(
            {'_id': station_id},
            {'$set': {
                'priority': priority,
                'updated_at': datetime.now()
            }}
        )

    def update_priority_of_bizcircle(self, bizcircle_id, priority):
        '''
        update priority of bizcircle
        '''
        self.bizcircles_col.update_one(
            {'_id': bizcircle_id},
            {'$set': {
                'priority': priority,
                'updated_at': datetime.now()
            }}
        )

    def _report_error(self, doc):
        '''
        error_source: 'validator' | 'url_crawler' | 'detail_crawler' | 'locate_element'
        message: 'elm_not_found' | 'invalid_value' | 'unexpected_error' | 'invalid_value
        checked: true | false
        page_source_id: null | object_id
        url: string
        ...payload: {
            ....
        }
        '''
        try:
            if doc.get('message') == 'elm_not_found':
                self._check_if_error_exists({
                    'message': doc.get('message'),
                    'payload.path': doc.get('payload').get('path')
                }, doc)

            if doc.get('message') == 'unexpected_error':
                self._check_if_error_exists({
                    'error_source': doc.get('error_source'),
                    'message': doc.get('message'),
                    'payload.error_message': doc.get('payload').get('error_message')
                }, doc)

            if doc.get('message') == 'invalid_value':
                self._check_if_error_exists({
                    'error_source': doc.get('error_source'),
                    'message': doc.get('message'),
                    'url': doc.get('url')
                }, doc)
            if doc.get('message') == 'no_proxy_available':
                self._check_if_error_exists({
                    'message': 'no_proxy_available'
                }, doc)
            if doc.get('message') == 'ip_blocked':
                self._check_if_error_exists({
                    'message': 'ip_blocked',
                }, doc)

            self.errors.insert_one(
                {**doc,
                 'checked': False,
                 'created_at': datetime.now(),
                 'updated_at': datetime.now()})

        except ErrorExistsException:
            pass

    def get_staging_apts_with_missing_info(self, limit=1000, exclude=[]):
        '''
        get apartments from staging with missing info
        '''
        arr = list(self.apartments_staging.aggregate([
            {
                '$match': {
                    'house_code': {'$nin': exclude},
                    'missing_info': True, 'updated_time': {'$lte': datetime.now()-timedelta(hours=24)},
                    '$or': [
                        {'failed_times': {'$exists': False}},
                        {'failed_times': {'$lt': 1}},
                        {'check_times': {'$exists': False}},
                        {'check_times': {'$lt': 3}}
                    ],
                }
            },
            {
                '$sort': {'created_at': -1}
            },
            {'$limit': limit}
        ]))
        for apt in arr:
            self.apartments_staging.update_one(
                {'_id': apt.get('_id')},
                {
                    '$set': {
                        'updated_time': datetime.now(),
                        'checked_times': apt.get('checked_times', 0)+1
                    }
                }
            )
        return arr

    def update_missing_info(self, apartment, updated):
        self.apartments_staging.update_one(
            {'_id': apartment.get('_id')},
            {'$set': {
                **updated,
                'updated_time': datetime.now()
            }}
        )

    def _check_if_error_exists(self, doc, origin):
        res = self.errors.find_one({
            **doc
        })
        if res:
            self.errors.update_one(
                {'_id': res.get('_id')},
                {'$set': {
                    **origin,
                    'times': res.get('times', 0)+1,
                    'updated_at': datetime.now()
                }}
            )
            raise ErrorExistsException()

    def update_failure_task(self, task,  err=None, page_source=None):
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
            # if page_source:
            # page_source_id = self.insert_page_source(
            #     task.get('url'), page_source)
            # toupdate['page_source_id'] = page_source_id

        self.tasks.update_one(
            {'_id': task.get('_id')},
            {'$set': {**toupdate, 'updated_at': datetime.now()}}
        )

        if err:
            self.report_unexpected_error_detail_crawler(err, task.get('url'))

    def report_unexpected_error(self, error_source, err, url=None):
        self._report_error({
            'error_source': error_source,
            'message': 'unexpected_error',
            'url': url,
            'payload': {
                'error_message': str(err),
                'error_stack': traceback.format_exc()
            }
        })

    def report_unexpected_error_detail_crawler(self, *args, **kwargs):
        return self.report_unexpected_error('detail_crawler', *args, **kwargs)

    # def report_error_detail_crawler(self, message, url, payload={}):
    #     return self._report_error({
    #         'error_source': 'detail_crawler',
    #         'url': url,
    #         'message': message,
    #         'payload': payload
    #     })

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
        if self.apartments_staging.find_one({
            'house_url': apartment_doc.get('house_url')
        }):
            return
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

    def insert_page_source(self, url, page_source):
        return None
        res = self.page_source.find_one({
            'url': url
        })
        if res:
            return res['_id']
        return self.page_source.insert_one({
            'source': page_source,
            'url': url,
            'created_at': datetime.now(),
            'updated_at': datetime.now()
        }).inserted_id

    def report_no_such_elm_error(self, method_name, path, url, page_source):
        inserted_id = self.insert_page_source(url, page_source)
        self._report_error({
            'error_source': 'locate_element',
            'message': 'elm_not_found',
            'page_source_id': inserted_id,
            'url': url,
            'payload': {
                'method_name': method_name,
                'path': path,
                'url': url
            }
        })

    def insert_into_pool(self, metadata, station_info=None):
        '''
        url     string unique
        source  beike
        city    shanghai | string
        status   idle | processing | done | error | expired
        failed_times number
        page_source: string
        station_info: object
        created_at  
        updated_at
        '''
        urls = [d.get('url') for d in metadata]

        def _get_urls_exists(col, key, _urls, conditions={}):
            arr = list(col.find({
                **conditions,
                '{}'.format(key): {
                    '$in': _urls,
                },
            }))
            return [o.get(key) for o in arr]

        urls_exist_in_task = _get_urls_exists(self.tasks, 'url', urls, {
            'created_at': {
                '$gte': datetime.now()-timedelta(days=30)
            },
        })
        urls_exist_in_staging = _get_urls_exists(
            self.apartments_staging, 'house_url', urls, {
                'created_time': {
                    '$gte': datetime.now()-timedelta(days=60)
                },
            })
        urls_exist_in_apartment = _get_urls_exists(
            self.apartments, 'house_url', urls, {
                'created_time': {
                    '$gte': datetime.now()-timedelta(days=60)
                },
            })

        all_existing_urls = set(
            [*urls_exist_in_task, *urls_exist_in_staging, *urls_exist_in_apartment])

        new_tasks = filter(lambda o: o.get(
            'url') not in all_existing_urls, metadata)

        # existing_tasks = filter(lambda o: o.get(
        #     'url') in all_existing_urls, metadata)

        new_tasks = [{
            **o,
            'status': 'idle',
            'failed_times': 0,
            'created_at': datetime.now(),
            'updated_at': datetime.now()
        } for o in new_tasks]
        if len(new_tasks):
            self.tasks.insert_many(new_tasks)

        return [o.get('url') for o in new_tasks]

    def report_unexpected_error_url_crawler(self, *args):
        return self.report_unexpected_error('url_crawler', *args)

    def is_valid_district(self, district):
        return bool(self.bizcircles_col.find_one({
            'district_name': district
        }))

    def is_valid_bizcircle(self, name):
        return bool(self.bizcircles_col.find_one({
            'bizcircle_name': name
        }))

    def on_pass_validation(self, apartment):
        self.apartments_staging.delete_one({
            '_id': apartment.get('_id')
        })

        apartment = rm_useless_values(apartment)
        if not apartment.get('missing_info'):
            apartment['force_pass'] = True
        res = self.apartments.insert_one({
            **apartment,
            'updated_time': datetime.now()
        })
        return res.inserted_id

    def clean_tasks_stuck_on_processing(self):
        self.tasks.update_many(
            {'status': 'processing', 'updated_at': {
                '$lte':  datetime.now()-timedelta(minutes=5)}},
            {'$set': {
                'status': 'idle',
                'updated_at': datetime.now()
            }}
        )

    def get_unchecked_staging_apts(self):
        self.clean_tasks_stuck_on_processing()
        res = self.apartments_staging.find({
            '$or': [
                {'failed_times': {'$exists': False}, 'missing_info': False},
                {'failed_times': {'$lt': 1}, 'missing_info': False}
            ]
        })
        to_force_pass = self.apartments_staging.find(
            {'checked_times': {'$gte': 5}, 'missing_info': True}
        )
        for apt in to_force_pass:
            self.on_pass_validation(apt)
        return list(res)

    def report_error_ip_blocked(self, url, payload={}):
        self._report_error({
            'error_source': 'ip_blocked',
            'url': url,
            'message': 'ip_blocked',
            'payload': payload
        })

    def report_error_validator(self, message, url, payload={}):
        return self._report_error({
            'error_source': 'data_validator',
            'url': url,
            'message': message,
            'payload': payload
        })

    def report_invalid_value(self, apartment, invalid_value):
        self.apartments_staging.update_one(
            {'_id': apartment.get('_id')},
            {'$set': {
                'failed_times': 1,
                'updated_time': datetime.now()
            }}
        )
        self.report_error_validator('invalid_value', apartment.get('house_url'), {
            'url': apartment.get('house_url'),
            'invalid_value': invalid_value
        })

    def del_redundant_jobs_and_tasks(self):
        '''
        delete redundant jobs
        '''
        res = self.agenda_jobs_col.delete_many({
            'lastRunAt':{
                '$lt':datetime.now()-timedelta(days=2)
            }
        })
        return res.deleted_count


mongo = DB()
