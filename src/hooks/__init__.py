from .hook import Hook, HookHandler
from db.db import db
from exceptions import UrlExistsException, ApartmentExpiredException
from baiduMap.getCoordinates import getGeoInfo

def get_location_info(self, apartment_info):
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

class DefaultHooker(Hook):

    def on_get_url(self, url, station_info=None):
        exist = db.save_url(url, station_info)
        if exist:
            raise UrlExistsException()

    def on_get_apartment_info(self, info):
        if info is None:
            db.delete_apartment_from_url(info.get('house_url'))
            raise ApartmentExpiredException()
        else:
            doc = {**info, **get_location_info(info)}
            db.upsert_apartment(info.get('house_code'), doc)

    def on_url_expired(self, url):
        pass

class FormatData(Hook):
    def on_get_apartment_info(self, info):
        pass