from .hook import Hook, HookHandler
from db.db import db
from exceptions import UrlExistsException, ApartmentExpiredException
from baiduMap.getCoordinates import getGeoInfo

class DefaultHooker(Hook):

    def on_get_url(self, url, station_info=None):
        exist = db.save_url(url, station_info)
        if exist:
            raise UrlExistsException()

    def on_get_apartment_info(self, info, location_info=None):
        if info is None:
            db.delete_apartment_from_url(info.get('house_url'))
            raise ApartmentExpiredException()
        else:
            doc = {**info, **(location_info or {})}
            db.upsert_apartment(info.get('house_code'), doc)

    def on_url_expired(self, url):
        pass


class FormatData(Hook):
    def on_get_apartment_info(self, info, location_info=None):
        if not info: return
        
