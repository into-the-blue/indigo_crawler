import requests
import os
ak = os.getenv('BAIDU_MAP_ACCESS_KEY')
BAIDU_API_URL = 'http://api.map.baidu.com/geocoding/v3/'


def getCoordinatesFromAddress(address, city=None):
    '''
    http://lbsyun.baidu.com/index.php?title=webapi/guide/webservice-geocoding
    {
    'status': 0, 
    'result': {
        'location': {'lng': 121.47164895306997, 'lat': 31.256403111266106}, 
        'precise': 1, 
        'confidence': 75, 
        'comprehension': 100, 
        'level': '地产小区'
        }
    }
    '''
    res = requests.get(BAIDU_API_URL, {
        'address': address,
        'ak': ak,
        'city': city,
        'output': 'json'
    })
    return res.json()


def getGeoInfo(city, district, community_name, bizcircle):
    address = city+district+community_name
    result = getCoordinatesFromAddress(address, city).get('result')
    return result


def get_location_info_from_apartment_info(apartment_info):
    result = getGeoInfo(apartment_info.get('city'), apartment_info.get(
        'district'), apartment_info.get('community_name'), apartment_info.get('bizcircle'))
    lng = result['location']['lng']
    lat = result['location']['lat']
    location_info = {
        'lng': lng,
        'lat': lat,
        'geo_info': result
    }
    return location_info
