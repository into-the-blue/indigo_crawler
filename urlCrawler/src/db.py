from common.db import DB
from datetime import datetime


class MyDB(DB):
    def __init__(self):
        super().__init__()

    def insert_into_pool(self, metadata):
        '''
        url     string unique
        source  beike
        city    shanghai | string
        status   idle | processing | done
        failed_times number
        source_page: string
        station_info: object
        created_at  
        updated_at
        '''
        if self.tasks.find_one({
            'url': metadata.get('url')
        }):
            return

        if self.apartments_staging.find_one({
            'house_url': metadata.get('url')
        }):
            return

        if self.apartments.find_one({
            'house_url': metadata.get('url')
        }):
            return

        self.apartments.insert_one({
            **metadata,
            'status': 'idle',
            'failed_times': 0,
            'created_at': datetime.now(),
            'updated_at': datetime.now()
        })


db = MyDB()
