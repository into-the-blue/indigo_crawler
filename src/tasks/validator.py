import re
from exceptions import ValidatorInvalidValueException
from db import mongo


def list_validator(arr):
    return lambda x: x in arr


def or_validator(*args):
    return lambda x: True in [f(x) for f in args]


def regex_validator(reg):
    return lambda x: False if not isinstance(x, str) else bool(re.match(reg, x))


def func_validator(func):
    return bool(lambda x: func(x))


def type_validator(*ts):
    return lambda x: type(x) in ts


def iterable_validator(validator):
    return lambda vs: len([b for b in [validator(v) for v in vs] if b is False]) == 0


boolean_validator = list_validator((0, 1))
url_validator = regex_validator('http[s]:\/\/.+(com|cn).+')
date_validator = regex_validator('\d{4}-\d{2}-\d{2}')


validator = {
    'air_condition': boolean_validator,
    'bed': boolean_validator,
    'fridge': boolean_validator,
    'heating': boolean_validator,
    'natural_gas': boolean_validator,
    'television': boolean_validator,
    'washing_machine': boolean_validator,
    'water_heater': boolean_validator,
    'wifi': boolean_validator,
    'subway_accessibility': boolean_validator,
    'floor_accessibility': boolean_validator,
    'carport': list_validator(('暂无数据', '租用车位', '免费使用')),
    'check_in_date': or_validator(date_validator, list_validator(['暂无数据', '随时入住'])),
    'closet': boolean_validator,
    'electricity': list_validator(['暂无数据', '民电', '商电']),
    'elevator': list_validator(['无', '有', '暂无数据']),
    'gas': list_validator(['有', '无', '暂无数据']),
    # beijing specific, \d居室
    'house_type': or_validator(regex_validator('[\d未知]{1,2}室[\d未知]{1,2}厅[\d未知]{1,2}卫'), regex_validator('[\d未知]居室')),
    'orient': or_validator(regex_validator('(未知)?[东南西北\s]*$'), list_validator(['暂无数据', '--'])),
    'type': list_validator(['合租', '整租']),
    'water': list_validator(['民水', '商水', '暂无数据']),
    'area': type_validator(int),
    'building_total_floors': or_validator(type_validator(int, str), list_validator(['暂无数据'])),
    'city': type_validator(str),
    'city_abbreviation': type_validator(str),
    'community_name': type_validator(str),
    'community_url': url_validator,
    'created_at': date_validator,
    'floor': or_validator(regex_validator('^\d{1,2}$'), list_validator(['暂无数据', '低楼层', '地下', '中楼层', '地下室', '高楼层', '未知'])),
    'floor_full_info': type_validator(str),
    'lease': or_validator(regex_validator('[月年]?'), list_validator(['暂无数据'])),
    'price': type_validator(int),
    'price_per_square_meter': type_validator(float),
    'tags': iterable_validator(type_validator(str)),
    'title': type_validator(str),
    'transportations': iterable_validator(lambda x: len(x) == 2)
}


GUANG_ZHOU_DISTRICTS = ['南海']

SU_ZHOU_BIZCIRCLES = ['玉山城南', '玉山城北', '玉山老城区', '玉山城西', '玉山城东', '玉山南星渎']

HANG_ZHOU_DISTRICTS = ['海宁市']


def district_validator(district):
    if district in ['上海周边', *GUANG_ZHOU_DISTRICTS, *HANG_ZHOU_DISTRICTS]:
        return True
    return mongo.is_valid_district(district)


def bizcircle_validator(biz):
    if biz in ['昆山', '嘉兴', '启东', '上海周边', *SU_ZHOU_BIZCIRCLES]:
        return True
    return mongo.is_valid_bizcircle(biz)


sh_validator = {
    **validator,
    'district': district_validator,
    'bizcircle': bizcircle_validator,
}


def get_validator(city_abbreviation):
    return sh_validator


def examine_apartment(apartment):
    _validator = get_validator(apartment.get('city_abbreviation'))
    invalid_data = [{'{}'.format(k): apartment.get(k)}
                    for k, v in _validator.items()
                    if not v(apartment.get(k))]
    if len(invalid_data) > 0:
        raise ValidatorInvalidValueException(invalid_data)
