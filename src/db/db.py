import pymongo
import os
from utils.util import extract_house_id, extract_house_code_from_url, currentDate
import datetime
db_username = os.getenv('DB_USERNAME')
db_password = os.getenv('DB_PASSWORD')
db_host = os.getenv('DB_HOST')
'''
{'type': '整租',
 'title': '整租·骏豪国际 1室1厅 南',
 'created_at': '2019-08-20',
 'house_code': 'SH2327594566997516288',
 'house_id': '2327594566997516288',
 'city_abbreviation': 'sh',
 'img_urls': ['https://ke-image.ljcdn.com/310000-inspection/test-94fe60ba-1125-458d-959e-fffe7451f46e.png.780x439.jpg',
  'https://ke-image.ljcdn.com/hdic-frame/test-27a20597-6661-454d-a3e0-d477153c7978.png.780x439.jpg',
  'https://ke-image.ljcdn.com/310000-inspection/test-6dfcf94f-7af0-4b36-9bf9-9e175c4436b6.png.780x439.jpg',
  'https://ke-image.ljcdn.com/310000-inspection/test-e2c9fa5e-8e27-4275-a8ad-4076c0c5d1f4.jpg.780x439.jpg',
  'https://ke-image.ljcdn.com/310000-inspection/test-e6fa5bbe-1081-46d8-a2c4-78337639e77f.jpg.780x439.jpg'],
 'price': 6500,
 'tags': ['近地铁', '精装'],
 'house_type': '1室1厅1卫',
 'area': 52,
 'orient': '南',
 'broker_name': '',
 'broker_contact': '',
 'minimal_lease': '2年以内',
 'maximal_lease': '2年以内',
 'floor': '中楼层',
 'building_total_floors': 29,
 'carport': '暂无数据',
 'electricity_type': '民电',
 'check_in_date': '随时入住',
 'reservation': '需提前预约',
 'elevator': '有',
 'water': '民水',
 'gas': '无',
 'television': 1,
 'fridge': 1,
 'washing_machine': 1,
 'air_condition': 1,
 'water_heater': 1,
 'bed': 1,
 'heating': 1,
 'wifi': 0,
 'closet': 1,
 'natural_gas': 0,
 'transportations': [['2号线,12号线,13号线', '南京西路', 545], ['2号线,7号线', '静安寺', 898]],
 'community_deals': '',
 'house_description': '',
 'house_url': 'https://sh.zu.ke.com/zufang/SH2327594566997516288.html',
 'city': '上海',
 'district': '静安',
 'bizcircle': '南京西路',
 'community_name': '骏豪国际',
 'community_url': 'https://sh.zu.ke.com/zufang/c5011000018078/',
 'price_per_square_meter': 125.0,
 'broker_brand': '链家',
 'floor_accessibility': 1,
 'subway_accessibility': 1}
'''


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

    def find_apartment_by_house_id(self, house_id):
        return self.apartments.find_one(
            {'house_id': house_id, 'title': {'$exists': True}})

    def exist_apartment(self, house_code):
        house_id = extract_house_id(house_code)
        res = self.find_apartment_by_house_id(house_id)
        return bool(res)

    def exist_apartment_from_url(self, url):
        house_code = extract_house_code_from_url(url)
        return self.exist_apartment(house_code)

    def update_apartment(self, house_id, doc, upsert=False):
        res = self.apartments.update_one({'house_id': house_id}, {
            '$set': doc
        }, upsert=upsert)

    def upsert_apartment(self, house_code, doc):
        house_id = extract_house_id(house_code)
        self.update_apartment(house_id, doc, upsert=True)

        # return self.apartments.replace_one({'house_id': house_id}, doc, upsert=True)

    def delete_apartment_from_house_id(self, house_id):
        return self.apartments.delete_one({'house_id': house_id, 'title': {'$exists': False}})

    def delete_apartment_from_url(self, url):
        house_code = extract_house_code_from_url(url)
        house_id = extract_house_id(house_code)
        if(self.exist_apartment(house_code)):
            self.apartments.update_one(
                {'house_id': house_id}, {'$set': {'expired': True}})
        self.delete_apartment_from_house_id(house_id)

    def save_url_with_station(self, url, station_info):
        house_code = extract_house_code_from_url(url)
        house_id = extract_house_id(house_code)
        station_id = station_info.get('station_id')
        line_id = station_info.get('line_id')
        if(self.exist_apartment(house_code)):
            res = self.find_apartment_by_house_id(house_id)
            station_ids = res.get('station_ids', [])
            line_ids = res.get('line_ids', [])
            if (line_id not in line_ids) or (station_id not in station_ids):
                station_ids.append(station_id)
                station_ids = list(set(station_ids))
                line_ids.append(line_id)
                line_ids = list(set(line_ids))
                self.update_apartment(
                    house_id, {'line_ids': line_ids, 'station_ids': station_ids})
            return True
        else:
            self.apartments.insert_one({
                'house_url': url,
                'house_code': house_code,
                'house_id': house_id,
                'station_ids': [station_id],
                'line_ids': [line_id]
            })
            return False

    def save_url(self, url, station_info=None):
        if(station_info is not None):
            return self.save_url_with_station(url, station_info)
        house_code = extract_house_code_from_url(url)
        house_id = extract_house_id(house_code)
        if (self.exist_apartment(house_code)):
            return True
        else:
            self.apartments.insert_one({
                'house_url': url,
                'house_code': house_code,
                'house_id': house_id
            })
            return False

    def find_missing_apartments(self):
        res = self.apartments.find({'title': {"$exists": False}})
        return list(map(lambda x: x.get('house_url'), res))

    def find_all_stations(self):
        return list(self.station_col.find({'city': 'sh'}))

    def start_cron_job(self, job_type, status='running'):
        return self.cronjob.insert_one({'job_type': job_type, 'start_time': currentDate(), 'status': 'running', 'error': None}).inserted_id

    def update_cron_job(self, _id, status='done', error=None):
        self.cronjob.update_one({'_id': _id}, {'$set': {
            'status': status,
            'error': error,
            'end_time': currentDate()
        }})

    def findApartmentsWithoutCoor(self, limit=50):
        return self.apartments.find({'lat': {'$exists': False}, 'title': {'$exists': True}}).limit(limit)


db = DB()