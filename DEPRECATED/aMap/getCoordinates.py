import requests
import os
# ak = os.getenv('BAIDU_MAP_ACCESS_KEY')
# BAIDU_API_URL = 'http://api.map.baidu.com/geocoding/v3/'


# def getCoordinatesFromAddress(address, city=None):
#     '''
#     http://lbsyun.baidu.com/index.php?title=webapi/guide/webservice-geocoding
#     {
#     'status': 0,
#     'result': {
#         'location': {'lng': 121.47164895306997, 'lat': 31.256403111266106},
#         'precise': 1,
#         'confidence': 75,
#         'comprehension': 100,
#         'level': '地产小区'
#         }
#     }

#     use gcj02ll
#     '''
#     res = requests.get(BAIDU_API_URL, {
#         'address': address,
#         'ak': ak,
#         'city': city,
#         'output': 'json',
#         'ret_coordtype': 'gcj02ll'
#     })
#     return res.json()


# def getGeoInfo(city, district, community_name, bizcircle):
#     address = city+district+community_name
#     result = getCoordinatesFromAddress(address, city).get('result')
#     return result


# def get_location_info_from_apartment_info(apartment_info):
#     result = getGeoInfo(apartment_info.get('city'), apartment_info.get(
#         'district'), apartment_info.get('community_name'), apartment_info.get('bizcircle'))
#     lng = result['location']['lng']
#     lat = result['location']['lat']
#     location_info = {
#         'lng': lng,
#         'lat': lat,
#         'geo_info': result,
#         'coordtype': 'gcj02',
#         'coordinates': [lng, lat]
#     }
#     return location_info
AMAP_API_URL = 'https://restapi.amap.com/v3/geocode/geo?parameters'
AMAP_AK = os.getenv('AMAP_ACCESS_KEY')


def decode_addr_amap(address, city=None):
    '''
    https://lbs.amap.com/api/webservice/guide/api/georegeo/?sug_index=3
    {'status': '1',
     'info': 'OK',
     'infocode': '10000',
     'count': '1',
     'geocodes': [{'formatted_address': '上海市长宁区虹桥路地铁站',
       'country': '中国',
       'province': '上海市',
       'citycode': '021',
       'city': '上海市',
       'district': '长宁区',
       'township': [],
       'neighborhood': {'name': [], 'type': []},
       'building': {'name': [], 'type': []},
       'adcode': '310105',
       'street': [],
       'number': [],
       'location': '121.420814,31.197524',
       'level': '兴趣点'}]}
    '''
    res = requests.get(AMAP_API_URL, {
        'address': address,
        'key': AMAP_AK,
        'city': city,
        'output': 'json'
    })
    return res.json()


def getGeoInfo(city, district, community_name, bizcircle):
    address = city+district+community_name
    result = decode_addr_amap(address, city)
    return result


def get_location_info_from_apartment_info(apartment_info):
    '''
    if no result:
        return None of each attribute
    '''
    try:
        result = getGeoInfo(apartment_info.get('city'), apartment_info.get(
            'district'), apartment_info.get('community_name'), apartment_info.get('bizcircle'))
        coordinates = [float(n) for n in result.get(
            'geocodes')[0].get('location').split(',')]
        location_info = {
            'lng': coordinates[0],
            'lat': coordinates[1],
            'geo_info': result,
            'coordtype': 'amap-gcj02',
            'coordinates': coordinates
        }
        return location_info
    except Exception as e:
        return {
            'lng': None,
            'lat': None,
            'geo_info': None,
            'coordtype': None,
            'coordinates': None
        }
