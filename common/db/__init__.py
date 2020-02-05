import pymongo
import os
from pymongo import IndexModel, ASCENDING, DESCENDING, GEOSPHERE
from datetime import datetime
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

        ensure_indexes_of_station(self.station_col)
        ensure_indexes_of_apartment(self.apartments)
        ensure_indexes_of_apartment(self.apartments_staging)

    def find_all_stations(self, city):
        return list(self.station_col.find({'city': city}))

    def report_error(self, doc):
        '''
        error_type: 'validator' | 'url_crawler' | 'detail_crawler'
        message: 'elm_not_found' | 'invalid_value' | ''
        checked: true | false
        ...payload: {
            ....
        }
        '''
        self.errors.insert_one(
            {**doc,
            'checked': False,
            'created_at': datetime.now(),
            'updated_at': datetime.now()})
