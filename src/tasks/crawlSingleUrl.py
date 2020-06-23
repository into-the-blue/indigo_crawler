import pandas as pd
import re
from bs4 import BeautifulSoup
from utils.logger import logger
from time import sleep
import os
from selenium.common.exceptions import NoSuchElementException
from locateElms import find_apartments_in_list
from exceptions import ApartmentExpiredException
from locateElms import find_elm_of_house_type, find_elm_of_basic_detail, find_elm_of_facility_detail, find_city_elm, find_district_elm, find_community_elm, find_bizcircle_elm, locate_elm_of_offline_text, locate_apartment_title, locate_updated_at, locate_house_code, locate_img_list, locate_price, locate_tags, locate_house_desc, locate_transportation


def get_house_type_info_27_nov(driver):
    type_area_text = find_elm_of_house_type(driver, 1)
    house_type = type_area_text.split(' ')[0]
    area = int(re.findall('\d+', type_area_text.split(' ')[1])[0])
    orient = find_elm_of_house_type(driver, 2)
    return house_type, area, orient


def get_info_1(driver):
    info_elms = find_elm_of_basic_detail(driver)
    all_texts = [elm.text for elm in info_elms if len(elm.text.strip())]

    def _match_text(txt, keyword, obj=None, obj_key=None):
        if keyword in txt:
            if obj is None:
                return txt.replace(keyword, '').strip()
            obj[obj_key] = txt.replace(keyword, '').strip()
        return None

    info = {
        'area': None,
        'orient': None,
        'floor_full_info': None,
        'floor': None,
        'building_total_floors': None,
        'lease': None,
        'carport': None,
        'electricity': None,
        'check_in_date': None,
        #         'reservation': None,
        'elevator': None,
        'water': None,
        'gas': None,
        'heating': None
    }
    for txt in all_texts:
         # area
        if '面积' in txt:
            try:
                info['area'] = int(re.findall('\d{1,3}', txt)[0])
            except:
                pass

        # orient
        _match_text(txt, '朝向：', info, 'orient')

        # floor
        if '楼层' in txt:
            floor_full_info = _match_text(txt, '楼层：')
            info['floor_full_info'] = floor_full_info
            floor = None
            building_total_floors = None
            if(floor_full_info.find('/') != -1):
                # 所在楼层
                floor = floor_full_info.split('/')[0]
                # 总楼层
                building_total_floors = int(
                    floor_full_info.split('/')[1].replace('层', ''))
            else:
                # 所在楼层
                res = re.findall('\D+(?=\d)', floor_full_info)
                if(len(res) > 0):
                    floor = res[0]
                else:
                    floor = floor_full_info
                # 总楼层
                res2 = re.findall('\d+', floor_full_info)
                if(len(res2) > 0):
                    building_total_floors = res2[0]
                else:
                    building_total_floors = floor_full_info
            info['floor'] = floor
            info['building_total_floors'] = building_total_floors

        # lease
        _match_text(txt, '租期：', info, 'lease')

        # carport
        _match_text(txt, '车位：', info, 'carport')

        # water type
        _match_text(txt, '用水：', info, 'water')

        # electricity
        _match_text(txt, '用电：', info, 'electricity')

        # gas
        _match_text(txt, '燃气：', info, 'gas')

        # heating
        _match_text(txt, '采暖：', info, 'heating')

        # elevator
        _match_text(txt, '电梯：', info, 'elevator')

        # check in date
        _match_text(txt, '入住：', info, 'check_in_date')

    floor_accessibility = 0
    if info.get('elevator') == '有':
        floor_accessibility = 1
    elif info.get('elevator') == '无' and info.get('floor') <= '3':
        floor_accessibility = 1
    elif info.get('elevator') == 0 and info.get('floor') == '低楼层':
        floor_accessibility = 1
    else:
        pass
    return {
        **info,
        'floor_accessibility': floor_accessibility
    }


def get_facility_info(driver):
    elms = find_elm_of_facility_detail(driver)
    info = {
        'television': 0,
        'fridge': 0,
        'washing_machine': 0,
        'air_condition': 0,
        'water_heater': 0,
        'bed': 0,
        'heating': 0,
        'wifi': 0,
        'closet': 0,
        'natural_gas': 0,
    }

    def _matcher(elm, keyword, obj, key):
        if keyword in elm.text:
            obj[key] = 1 if '_no' not in elm.get_attribute('class') else 0

    for elm in elms:
        _matcher(elm, '电视', info, 'television')
        _matcher(elm, '冰箱', info, 'fridge')
        _matcher(elm, '洗衣机', info, 'washing_machine')
        _matcher(elm, '空调', info, 'air_condition')
        _matcher(elm, '热水器', info, 'water_heater')
        _matcher(elm, '床', info, 'bed')
        _matcher(elm, '暖气', info, 'heating')
        _matcher(elm, '宽带', info, 'wifi')
        _matcher(elm, '衣柜', info, 'closet')
        _matcher(elm, '天然气', info, 'natural_gas')

    return info


def get_community_info(driver):
     # 城市
    city = find_city_elm(driver).text[:-3]

    # 城区
    district = find_district_elm(driver).text[:-2]

    # 商圈
    bizcircle = find_bizcircle_elm(driver).text[:-2]

    community_elm = find_community_elm(driver)
    # 小区名称
    community_name = community_elm.text[:-2]

    # 小区链接
    community_url = community_elm.get_attribute('href')

    return {
        'city': city,
        'district': district,
        'bizcircle': bizcircle.strip(),
        'community_name': community_name,
        'community_url': community_url
    }


def get_transportation_info(driver):
    elms = locate_transportation(driver)
    transportations = []
    subway_accessibility = 0
    for elm in elms:
        spans = elm.find_elements_by_xpath('./span')
        if len(spans):
            line_info = [e.text for e in spans if len(spans)]
            distances = [int(re.findall('\d{1,4}', txt)[0]) for txt in line_info if len(
                re.findall('^\d{2,4}', txt))]
            subway_accessibility = subway_accessibility or distances[0] < 1000 if len(
                distances) else 0
            transportations.append(line_info)
    return {
        'transportations': transportations,
        'subway_accessibility': int(subway_accessibility)
    }


def get_info_of_single_url(driver, url):
    """
    get info of single url
    """
    # TODO：支持对公寓房源的爬取。
    if 'apartment' in url:
        pass
    else:
        try:
            # url expired
            offline_texts = ['你访问的房源已失效', '你访问的房源已下架/成交']
            offline_text = locate_elm_of_offline_text(driver).text
            if offline_text in offline_texts or len(offline_text) > 0:
                raise ApartmentExpiredException()
        except NoSuchElementException:
            pass

        # scroll to end of the page to avoid lazy rendering
        sleep(1)

        missing_info = False

        driver.execute_script(
            "window.scrollTo(0, document.body.scrollHeight);")

        # get rent type from title
        # eg.合租·瑞和城叁街区(汇臻路815弄) 4居室 南卧
        title = locate_apartment_title(driver).text
        rent_type = title.split('·')[0]
        rent_type_fallback = find_elm_of_house_type(driver, 0)
        rent_type = rent_type if len(rent_type) <= len(
            rent_type_fallback) else rent_type_fallback
        if '未知' in rent_type:
            rent_type = '未知'
            missing_info = True

        # 上架时间
        time_listed = re.findall(
            '\d{4}-\d{2}-\d{2}', locate_updated_at(driver).text)[0]

        # 编号
        house_code = None
        try:
            house_code = re.findall(
                '[A-Z]+\d+', locate_house_code(driver).text)[0]
        except:
            house_code = re.findall(
                '\/([A-Z]{1,3}\d+)\.html', url)[0]
        house_id = re.findall('[0-9]+', house_code)[0]
        city_abbreviation = re.findall('[a-zA-Z]+', house_code)[0].lower()

        # 房源照片列表
        img_urls = []
        # lazy load return placeholder img
        # get thumbnail img
        for i in locate_img_list(driver):
            img_url = i.get_attribute('src')
            cover_img_size = '780x439'
            img_url = re.sub('\d{2,3}x\d{2,3}', cover_img_size, img_url)
            img_urls.append(img_url)
#             json_house_imgs = json.dumps(json_house_imgs, ensure_ascii=False)

        missing_info = missing_info or len(img_urls) < 2
        # 价格
        price = int(locate_price(driver).text)

        # 特色标签列表
        tags = []
        try:
            for i in locate_tags(driver):
                tags.append(i.text)
        except NoSuchElementException:
            missing_info = True
        # json_house_tags = json.dumps(json_house_tags, ensure_ascii=False)

        # 户型、面积、朝向
        house_type, area, orient = get_house_type_info_27_nov(driver)

        if '未知' in orient:
            missing_info = True
        content_article_info = get_info_1(driver)

        content_article_info['area'] = content_article_info['area'] or area
        missing_info = missing_info or not content_article_info['area']

        facility_info = get_facility_info(driver)

        community_info = {}
        try:
            # can be null
            community_info = get_community_info(driver)
            if not community_info.get('city'):
                missing_info = True
        except:
            missing_info = True

        # 地址和交通，地铁便利性
        # TODO:地铁便利性的筛选标准，距任一地铁站的距离有小于1000m的

        transportation_info = get_transportation_info(driver)

        # 小区最新成交
        try:
            community_deals = driver.find_element_by_xpath(
                "//div[@class='table']").get_attribute('innerHTML')
            table = BeautifulSoup(community_deals, 'lxml')
            record = []
            # 表格内容
            for tr in table.find_all("div", class_='tr')[1:]:
                cols = tr.find_all("div", class_='td')
                cols = [ele.text.strip() for ele in cols]
                # Get rid of empty values
                record.append([ele for ele in cols if ele])
            community_deals = pd.DataFrame(
                data=record,
                columns=['成交日期', '居室', '面积', '租赁方式', '出租价格']
            ).to_json(orient='records', force_ascii=False)
        except:
            # _print("无小区最新成交信息：", url)
            community_deals = ''

        # 房源描述
        house_description = ''
        try:
            desc = locate_house_desc(
                driver).get_attribute('data-desc')
            if desc:
                house_description = desc
        except:
            pass
        # if len(driver.find_elements_by_xpath("//div[@class='content__article__info3 ']/ul/li/p")) > 2:
        #     house_description = ''
        #     logger.info(
        #         f"请调整子函数get_list_info的房源描述部分，有超过一条评论的情况需要全部考虑。（做成列表而不再是文本）{url}")
        # else:
        #     try:
        #         house_description = driver.find_element_by_xpath(
        #             "//div[@class='content__article__info3 ']/ul/li/p[1]").text
        #     except:
        #         house_description = ''

        # 每平米房价
        try:
            price_per_square_meter = round(
                price/content_article_info.get('area'), 2)
        except:
            logger.info(f'无法计算每平米房价：{url}',)
            price_per_square_meter = float(price)

        # 上下楼便利性：无障碍性，楼层与电梯的合成项

        # 导出所有信息
        dict_single = {'type': rent_type,
                       'title': title,
                       'created_at': time_listed,
                       'house_code': house_code,
                       'house_id': house_id,
                       'city_abbreviation': city_abbreviation,
                       'img_urls': img_urls,
                       'price': price,
                       'tags': tags,
                       'house_type': house_type,
                       **content_article_info,
                       **facility_info,
                       **transportation_info,
                       'community_deals': community_deals,
                       'house_description': house_description,
                       'house_url': url,
                       **community_info,
                       'price_per_square_meter': price_per_square_meter,
                       'missing_info': (len(img_urls) < 2) or (len(rent_type) < 2),
                       'version': os.environ.get('VER')
                       }
        return dict_single
