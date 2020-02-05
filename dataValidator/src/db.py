from common.db import DB
from datetime import datetime
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
        res = self.apartments_staging.find_one({
            '$or': [
                {'failed_times': {'$exists': False}, 'missing_info': False},
                {'failed_times': {'$lt': 1}, 'missing_info': False}
            ]
        })
        return res

    def get_missing_info(self):
        res = self.apartments_staging.find_one({
            'missing_info': True
        })
        return res

    def update_missing_info(self, apartment, updated):
        self.apartments_staging.update_one({
            {'_id': apartment.get('_id')},
            {'$set': {
                **updated,
                'updated_time': datetime.now()
            }}
        })

    def on_pass_validation(self, apartment):
        self.apartments_staging.delete_one({
            '_id': apartment.get('_id')
        })

        apartment = rm_useless_values(apartment)

        self.apartments.insert_one({
            **apartment,
            'updated_time': datetime.now()
        })

    def report_error(self, message, payload):
        return super().report_error({
            'error_type': 'validator',
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
        self.report_error('invalid_value', {
            'apartment_url': apartment.get('house_url'),
            'invalid_value': invalid_value
        })


db = MyDB()
