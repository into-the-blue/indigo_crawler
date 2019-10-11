import requests
import os
ak = os.environ.get('BAIDU_MAP_ACCESS_KEY','Z83cQAwSnsI767Kh0nyplLD4yXbIWoZ8')
BAIDU_API_URL = 'http://api.map.baidu.com/geocoding/v3/?address={address}&output=json&ak={ak}'


def getCoordinatesFromAddress(address):
    '''
    {
    'status': 0, 
    'result': 
    {'location': 
        {'lng': 121.47164895306997, 'lat': 31.256403111266106}, 
    'precise': 1, 
    'confidence': 75, 
    'comprehension': 100, 
    'level': '地产小区'}
    }
    '''
    res = requests.get(BAIDU_API_URL.format(address=address,ak=ak))
    return res.json()