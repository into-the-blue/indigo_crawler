from common.db import DB
from datetime import datetime
from common.utils.logger import logger


class MyDB(DB):
    def __init__(self):
        super().__init__()

    def insert_into_pool(self, metadata):
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
        if self.tasks.find_one({
            'url': metadata.get('url')
        }):
            return False

        staging = self.apartments_staging.find_one({
            'house_url': metadata.get('url')
        })
        if staging:
            if metadata.get('station_info'):
                self._update_station_info_for_apartment(
                    self.apartments_staging, staging, metadata.get('station_info'))
            return False

        apartment = self.apartments.find_one({
            'house_url': metadata.get('url')
        })
        if apartment:
            if metadata.get('station_info'):
                self._update_station_info_for_apartment(
                    self.apartments, apartment, metadata.get('station_info'))
            return False

        self.tasks.insert_one({
            **metadata,
            'status': 'idle',
            'failed_times': 0,
            'created_at': datetime.now(),
            'updated_at': datetime.now()
        })
        return True

    def _update_station_info_for_apartment(self, col, apartment, station_info):
        '''
        update station info 
        check if station id and line ids exist in apartment info
        if not, add it
        '''
        line_ids = station_info.get('line_ids')
        station_id = station_info.get('station_id')
        _station_ids = apartment.get('station_ids', [])
        _line_ids = apartment.get('line_ids', [])
        if (len(list(set(line_ids) - set(_line_ids))) > 0) or (station_id not in _station_ids):
            _station_ids.append(station_id)
            _station_ids = list(set(_station_ids))
            _line_ids += line_ids
            _line_ids = list(set(_line_ids))
            col.update_one(
                {'_id', apartment.get('_id')}, {'$set': {'line_ids': _line_ids, 'station_ids': _station_ids}})

    def report_error(self, message, payload):
        return super()._report_error({
            'error_source': 'url_crawler',
            'message': message,
            'payload': payload
        })

    def report_unexpected_error(self, *args):
        return super().report_unexpected_error('url_crawler', *args)


db = MyDB()
