from selenium import webdriver
from ..locateElement import find_elms_of_line, find_elms_of_stations
import operator
from functools import reduce


def flatten(li):
    return reduce(operator.add, li)


def get_line_info_from_elm(elm):
    return {
        'line_name': elm.text,
        'line_id': elm.get_attribute('data-id'),
        'url': elm.find_element_by_xpath("./a").get_attribute('href'),
        'city': 'shanghai'
    }


def get_station_info_from_elm(elm):
    return {
        'station_id': elm.get_attribute('data-id'),
        'station_name': elm.text,
        'url': elm.find_element_by_xpath('./a').get_attribute('href'),
        'city': 'shanghai'
    }


def find_elm(arr, func):
    for i, elm in enumerate(arr):
        if func(elm):
            return elm, i
    return None, None


def uniq_list(arr):
    return list(set(arr))


def uniq_stations(stations):
    unique_stations = []
    for station in stations:
        found, idx = find_elm(unique_stations, lambda x: x.get(
            'station_id') == station.get('station_id'))
        if found:
            found['line_ids'] = uniq_list(
                [station['line_id'], *found['line_ids']])
            found['urls'] = uniq_list([station['url'], *found['urls']])
        else:
            unique_stations.append({
                **station,
                'line_ids': [station['line_id']],
                'urls': [station['url']]
            })
    return unique_stations


class SetupMetroInfo():
    def __init__(self, driver: webdriver, city_url: str, city_name: str, line_col, station_col):
        self.driver = driver
        self.city_url = city_url
        self.city_name = city_name
        self.line_col = line_col
        self.station_col = station_col

    def click_order_by_metro(self):
        self.driver.get(self.city_url)
        self.driver.find_element_by_link_text('按地铁线').click()

    def get_line_infos(self):
        line_elms = find_elms_of_line(self.driver)
        line_infos = [get_line_info_from_elm(elm) for elm in line_elms]

    def get_station_infos(self, line_infos):
        stations = []
        for line_info in line_infos:
            driver.get(line_info.get('url'))
            sleep(1)
            print(line_info.get('line_name'))
            station_elms = find_elms_of_stations(self.driver)
            for station_elm in station_elms[1:]:
                station_info = get_station_info_from_elm(station_elm)
                station_info = {
                    **station_info,
                    **{k: v for k, v in line_info.items() if k in ('line_id')}
                }
                stations.append(station_info)
        return stations

    def run(self):
        self.click_order_by_metro()
        line_infos = self.get_line_infos()
        station_infos = self.get_station_infos(line_infos)
        station_infos = uniq_stations(station_infos)
        # save to db
        self.line_col.insert_many(line_infos)
        self.station_col.insert_many(station_infos)


class SetupBizCircle():
    def __init__(self, driver, city_url, city_name, biz_col):
        self.driver = driver
        self.city_url = city_url
        self.city_name = city_name
        self.biz_col = biz_col

    def get_all_districts(self):
        self.driver.get(self.city_url)
        elms = self.driver.find_elements_by_xpath(
            "//ul[@data-target='area']/li")
        return [
            {
                'district_id': e.get_attribute('data-id'),
                'district_name': e.text,
                'district_url': e.find_element_by_xpath('./a').get_attribute('href'),
                'city': city
            }
            for e in elms[1:]
        ]

    def get_all_bizcircles(self, districts):
        def _each_dist(dist):
            self.driver.get(dist['district_url'])
            elms = self.driver.find_elements_by_xpath(
                "//ul[@data-target='area']/li[@data-type='bizcircle']")
            return [
                {
                    **dist,
                    'bizcircle_id': e.get_attribute('data-id'),
                    'bizcircle_name': e.text,
                    'bizcircle_url': e.find_element_by_xpath('./a').get_attribute('href')
                }
                for e in elms[1:]
            ]
        all_biz = [_each_dist(d) for d in districts]
        return flatten(all_biz)

    def run(self):
        districts = self.get_all_districts()
        bizcircles = self.get_all_bizcircles(districts)
        self.biz_col.insert_many(bizcircles)
