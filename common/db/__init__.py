import pymongo
import os
from pymongo import IndexModel, ASCENDING, DESCENDING, GEOSPHERE
from datetime import datetime
import traceback
db_username = os.getenv('DB_USERNAME')
db_password = os.getenv('DB_PASSWORD')
db_host = os.getenv('DB_HOST')


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
        self.client = pymongo.MongoClient(
            db_host+':27017', username=db_username, password=db_password)
        self.indigo = self.client.indigo

        self.line_col = self.indigo.lines
        self.station_col = self.indigo.stations
        self.apartments = self.indigo.apartments
        self.cronjob = self.indigo.cronjob

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
        ensure_indexes_of_station(self.station_col)
        ensure_indexes_of_apartment(self.apartments)
        ensure_indexes_of_apartment(self.apartments_staging)

    def find_all_stations(self, city):
        return list(self.station_col.find({
            '$query': {'city': city},
            '$orderby': {'priority': -1}
        }))

    def find_all_bizcircles(self, city):
        return list(self.bizcircles_col.find({
            '$query': {'city': city},
            '$orderby': {'priority': -1}
        }))

    def update_priority_of_station(self, station_info, priority):
        self.station_col.update_one(
            {'_id': station_info.get('_id')},
            {'$set': {
                'priority': priority
            }}
        )

    def update_priority_of_bizcircle(self, bizcircle_info, priority):
        self.bizcircles_col.update_one(
            {'_id': bizcircle_info.get('_id')},
            {'$set': {
                'priority': priority
            }}
        )

    def report_error(self, doc):
        '''
        error_source: 'validator' | 'url_crawler' | 'detail_crawler' | 'locate_element'
        message: 'elm_not_found' | 'invalid_value' | ''
        checked: true | false
        page_source_id: null | object_id
        url: string
        ...payload: {
            ....
        }
        '''
        self.errors.insert_one(
            {**doc,
             'checked': False,
             'created_at': datetime.now(),
             'updated_at': datetime.now()})

    def report_unexpected_error(self, error_source, err, url=None):
        self.report_error({
            'error_source': error_source,
            'message': 'unexpected_error',
            'url': url,
            'payload': {
                'error_message': str(err),
                'error_stack': traceback.format_exc()
            }
        })

    def insert_page_source(self, url, page_source):
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
        self.report_error({
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


db = DB()
