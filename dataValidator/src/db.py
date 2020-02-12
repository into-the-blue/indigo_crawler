from common.db import DB
from datetime import datetime, timedelta
from common.utils.logger import logger


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


class MyDB(DB):
    def __init__(self):
        super().__init__()

    def get_unchecked(self):
        self.clean_tasks_stuck_on_processing()
        res = self.apartments_staging.find({
            '$or': [
                {'check_times': {'$gte': 3}, 'missing_info': True},
                {'failed_times': {'$exists': False}, 'missing_info': False},
                {'failed_times': {'$lt': 1}, 'missing_info': False}
            ]
        })
        return list(res)

    def on_pass_validation(self, apartment):
        self.apartments_staging.delete_one({
            '_id': apartment.get('_id')
        })

        apartment = rm_useless_values(apartment)
        if not apartment.get('missing_info'):
            apartment['force_pass'] = True
        self.apartments.insert_one({
            **apartment,
            'updated_time': datetime.now()
        })

    def report_error(self, message, url, payload):
        return super()._report_error({
            'error_source': 'data_validator',
            'url': url,
            'message': message,
            'payload': payload
        })

    def clean_tasks_stuck_on_processing(self):
        self.tasks.update_many(
            {'status': 'processing', 'updated_at': {
                '$lte':  datetime.now()-timedelta(minutes=5)}},
            {'$set': {
                'status': 'idle',
                'updated_at': datetime.now()
            }}
        )

    def report_unexpected_error(self, *args):
        return super().report_unexpected_error('data_validator', *args)

    def report_invalid_value(self, apartment, invalid_value):
        self.apartments_staging.update_one(
            {'_id': apartment.get('_id')},
            {'$set': {
                'failed_times': 1,
                'updated_time': datetime.now()
            }}
        )
        self.report_error('invalid_value', apartment.get('house_url'), {
            'url': apartment.get('house_url'),
            'invalid_value': invalid_value
        })


db = MyDB()
