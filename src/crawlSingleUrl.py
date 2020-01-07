import pandas as pd
import re
from bs4 import BeautifulSoup
from utils.util import _print
from time import sleep
import os


def get_house_type_info_old(driver):
    try:
        house_type = driver.find_elements_by_xpath(
            "//p[@class='content__article__table']/span")[1].text
        area = int(driver.find_elements_by_xpath(
            "//p[@class='content__article__table']/span")[2].text.split('㎡')[0])
        orient = driver.find_elements_by_xpath(
            "//p[@class='content__article__table']/span")[3].text.split('朝')[1]

        return house_type, area, orient
    except Exception as e:
        print(e)
        return None, None, None


def get_house_type_info_27_nov(driver):
    def get_text(_elms, idx): return _elms[idx].text.replace(
        _elms[idx].find_element_by_xpath('./span').text, '')
    try:
        elms = driver.find_elements_by_xpath(
            "//ul[@class='content__aside__list']/li")
        type_area_text = get_text(elms, 1)
        house_type = type_area_text.split(' ')[0]
        area = int(re.findall('\d+', type_area_text.split(' ')[1])[0])
        orient = get_text(elms, 2).split(' ')[0]
        return house_type, area, orient
    except Exception as e:
        print(e)
        return None, None, None

def get_info_1(driver):
    info_elms = driver.find_elements_by_xpath("//div[@class='content__article__info']/ul/li")
    all_texts = [elm.text for elm in info_elms if len(elm.text.strip())]
    
    def _match_text(txt,keyword,obj=None,obj_key=None):
        if keyword in txt:
            if obj is None: return txt.replace(keyword,'').strip()
            obj[obj_key] = txt.replace(keyword,'').strip()
        return None
    
    info = {
        'area':None,
        'orient': None,
        'floor_full_info':None,
        'floor': None,
        'building_total_floors': None,
        'lease': None,
        'carport': None,
        'electricity_type': None,
        'check_in_date': None,
#         'reservation': None,
        'elevator': None,
        'water': None,
        'gas': None,
        'heating':None
    }
    for txt in all_texts:
         # area
        if '面积' in txt:
            try:
                info['area'] = int(re.findall('\d{1,3}',txt)[0])
            except:
                pass
        
        # orient
        _match_text(txt,'朝向：',info,'orient')
            
        # floor
        if '楼层' in txt:
            floor_full_info = _match_text(txt,'楼层：')
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
                res = re.findall('\w+(?=\d)', floor_full_info)
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
        _match_text(txt,'租期：',info,'lease')
            
        # carport
        _match_text(txt,'车位：',info,'carport')
            
        # water type 
        _match_text(txt,'用水：',info,'water')
            
        # electricity type
        _match_text(txt,'用电：',info,'electricity')
            
        # gas 
        _match_text(txt,'燃气：',info,'gas')
            
        # heating
        _match_text(txt,'采暖：',info,'heating')
        
        # elevator
        _match_text(txt,'电梯：',info,'elevator')
            
        # check in date
        _match_text(txt,'入住：',info,'check_in_date')
            
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
    elms = driver.find_elements_by_xpath("//ul[@class='content__article__info2']/li")
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
    def _matcher(elm,keyword,obj,key):
        if keyword in elm.text:
            obj[key] =  1 if '_no' not in elm.get_attribute('class') else 0
            
    for elm in elms:
        _matcher(elm,'电视',info,'television')
        _matcher(elm,'冰箱',info,'fridge')
        _matcher(elm,'洗衣机',info,'washing_machine')
        _matcher(elm,'空调',info,'air_condition')
        _matcher(elm,'热水器',info,'water_heater')
        _matcher(elm,'床',info,'bed')
        _matcher(elm,'暖气',info,'heating')
        _matcher(elm,'宽带',info,'wifi')
        _matcher(elm,'衣柜',info,'closet')
        _matcher(elm,'天然气',info,'natural_gas')
    
    return info

def get_community_info(driver):
     # 城市
    city = driver.find_element_by_xpath(
        "//div[@class='bread__nav w1150 bread__nav--bottom']/p/a[1]").text[:-3]

    # 城区
    district = driver.find_element_by_xpath(
        "//div[@class='bread__nav w1150 bread__nav--bottom']/p/a[2]").text[:-2]

    # 商圈
    bizcircle = driver.find_element_by_xpath(
        "//div[@class='bread__nav w1150 bread__nav--bottom']/p/a[3]").text[:-2]

    # 小区名称
    community_name = driver.find_element_by_xpath(
        "//div[@class='bread__nav w1150 bread__nav--bottom']/h1/a").text[:-2]

    
    # 小区链接
    community_url = driver.find_element_by_xpath(
        "//div[@class='bread__nav w1150 bread__nav--bottom']/h1/a").get_attribute('href')
    return {
        'city': city,
        'district': district,
        'bizcircle': bizcircle,
        'community_name': community_name,
        'community_url': community_url
    }

def get_transportation_info(driver):
    elms = driver.find_elements_by_xpath("//div[@class='content__article__info4 w1150']/ul/li")
    transportations = []
    subway_accessibility = 0
    for elm in elms:
        spans = elm.find_elements_by_xpath('./span')
        if len(spans):
            line_info = [e.text for e in spans if len(spans)]
            distances = [int(re.findall('\d{1,4}',txt)[0]) for txt in line_info if len(re.findall('^\d{2,4}',txt))]
            subway_accessibility = subway_accessibility or distances[0] < 1000 if len(distances) else 0
            transportations.append(line_info)
    return {
        'transportations':transportations,
        'subway_accessibility':int(subway_accessibility)
    }
# TODO: 公寓 error


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
            driver.find_element_by_xpath(
                "//p[@class='offline__title']").text == '你访问的房源已失效'
            return None
        except:
            pass
        # 类型/标题：可能为空，导致标题的爬取报错
        # try:
        #     rent_type = driver.find_element_by_xpath("//p[@class='content__title']").text.split(' · ')[0]
        #     title = driver.find_element_by_xpath("//p[@class='content__title']").text.split(' · ')[1]
        # except:
        #     rent_type = '未知'
        #     title = driver.find_element_by_xpath("//p[@class='content__title']").text

        # scroll to end of the page to avoid lazy rendering
        sleep(1)
        driver.execute_script(
            "window.scrollTo(0, document.body.scrollHeight);")

        # get rent type from title
        # eg.合租·瑞和城叁街区(汇臻路815弄) 4居室 南卧
        # rent_type = driver.find_elements_by_xpath(
        #     "//p[@class='content__article__table']/span")[0].text
        title = driver.find_element_by_xpath(
            "//p[@class='content__title']").text
        rent_type = title.split('·')[0]
        if '未知' in rent_type:
            rent_type = '未知'

        # 上架时间
        time_listed = driver.find_element_by_xpath(
            "//div[@class='content__subtitle']").text[7:17]

        # 编号
        house_code = driver.find_element_by_xpath(
            "//div[@class='content__subtitle']/i[@class='house_code']").text[5:]
        house_id = re.findall('[0-9]+', house_code)[0]
        city_abbreviation = re.findall('[a-zA-Z]+', house_code)[0].lower()

        # 房源照片列表
        img_urls = []
        # lazy load return placeholder img
        # get thumbnail img
#             for i in driver.find_elements_by_xpath("//ul[@class='content__article__slide__wrapper']/div[@class='content__article__slide__item']/img"):
        for i in driver.find_elements_by_xpath("//div[@class='content__thumb--box']/ul[@class='content__article__slide--small content__article__slide_dot']/li/img"):
            img_url = i.get_attribute('src')
            cover_img_size = '780x439'
            img_url = re.sub('\d{2,3}x\d{2,3}', cover_img_size, img_url)
            img_urls.append(img_url)
#             json_house_imgs = json.dumps(json_house_imgs, ensure_ascii=False)

        # 价格
        price = int(driver.find_element_by_xpath(
            "//div[@class='content__aside--title']/span").text)

        # 特色标签列表
        tags = []
        for i in driver.find_elements_by_xpath("//p[@class='content__aside--tags']/i"):
            tags.append(i.text)
        # json_house_tags = json.dumps(json_house_tags, ensure_ascii=False)

        # 户型、面积、朝向
        house_type, area, orient = get_house_type_info_27_nov(driver)

        # 经纪人姓名：可能为空
        # TODO

        # 经纪人联系方式
        try:
            broker_contact = driver.find_element_by_xpath(
                "//p[@class='content__aside__list--bottom oneline']").text
        except:
            broker_contact = ''

        # 最短租期/最长租期:统一显示为天数，一年360天(防止12个月和365天天数不相等的情况)，一个月30天
#         rent_peroid = driver.find_element_by_xpath(
#             "//div[@class='content__article__info']/ul/li[5]").text[3:]
#         if '~' in rent_peroid:
#             if '月' in rent_peroid:
#                 minimal_lease = int(rent_peroid.split('~')[0])*30
#                 maximal_lease = int(rent_peroid.split('~')[
#                                     1].split('个月')[0])*30
#             elif '年' in rent_peroid:
#                 minimal_lease = int(rent_peroid.split('~')[0])*360
#                 maximal_lease = int(rent_peroid.split('~')[
#                                     1].split('年')[0])*360
#             else:
#                 _print("请手工检查租期：", rent_peroid, url)
#                 minimal_lease = rent_peroid
#                 maximal_lease = rent_peroid
#         else:
#             minimal_lease = rent_peroid
#             maximal_lease = rent_peroid

        # 楼层
#         building_floor_text = driver.find_element_by_xpath(
#             "//div[@class='content__article__info']/ul/li[8]").text[3:]
#         if(building_floor_text.find('/') != -1):
            # 所在楼层
#             floor = building_floor_text.split('/')[0]
            # 总楼层
#             building_total_floors = int(
#                 building_floor_text.split('/')[1].replace('层', ''))
#         else:
#             # 所在楼层
#             res = re.findall('\w+(?=\d)', building_floor_text)
#             if(len(res) > 0):
#                 floor = res[0]
#             else:
#                 floor = building_floor_text
#             # 总楼层
#             res2 = re.findall('\d+', building_floor_text)
#             if(len(res2) > 0):
#                 building_total_floors = res2[0]
#             else:
#                 building_total_floors = building_floor_text

        # 车位
#         carport = driver.find_element_by_xpath(
#             "//div[@class='content__article__info']/ul/li[11]").text[3:]

#         # 用电
#         electricity_type = driver.find_element_by_xpath(
#             "//div[@class='content__article__info']/ul/li[14]").text[3:]

#         # 入住
#         check_in_date = driver.find_element_by_xpath(
#             "//div[@class='content__article__info']/ul/li[3]").text[3:]

#         # 看房
#         reservation = driver.find_element_by_xpath(
#             "//div[@class='content__article__info']/ul/li[6]").text[3:]

#         # 电梯
#         elevator = driver.find_element_by_xpath(
#             "//div[@class='content__article__info']/ul/li[9]").text[3:]

#         # 用水
#         water = driver.find_element_by_xpath(
#             "//div[@class='content__article__info']/ul/li[12]").text[3:]

#         # 燃气
#         # TODO:和下面的天然气有什么区别？
#         gas = driver.find_element_by_xpath(
#             "//div[@class='content__article__info']/ul/li[15]").text[3:]
        content_article_info = get_info_1(driver)

        # 电视
#         television = driver.find_element_by_xpath(
#             "//ul[@class='content__article__info2']/li[2]").get_attribute('class')
#         if '_no' in television:
#             television = 0
#         else:
#             television = 1

#         # 冰箱
#         fridge = driver.find_element_by_xpath(
#             "//ul[@class='content__article__info2']/li[3]").get_attribute('class')
#         if '_no' in fridge:
#             fridge = 0
#         else:
#             fridge = 1

#         # 洗衣机
#         washing_machine = driver.find_element_by_xpath(
#             "//ul[@class='content__article__info2']/li[4]").get_attribute('class')
#         if '_no' in washing_machine:
#             washing_machine = 0
#         else:
#             washing_machine = 1

#         # 空调
#         air_condition = driver.find_element_by_xpath(
#             "//ul[@class='content__article__info2']/li[5]").get_attribute('class')
#         if '_no' in air_condition:
#             air_condition = 0
#         else:
#             air_condition = 1

#         # 热水器
#         water_heater = driver.find_element_by_xpath(
#             "//ul[@class='content__article__info2']/li[6]").get_attribute('class')
#         if '_no' in water_heater:
#             water_heater = 0
#         else:
#             water_heater = 1

#         # 床
#         bed = driver.find_element_by_xpath(
#             "//ul[@class='content__article__info2']/li[7]").get_attribute('class')
#         if '_no' in bed:
#             bed = 0
#         else:
#             bed = 1

#         # 暖气
#         heating = driver.find_element_by_xpath(
#             "//ul[@class='content__article__info2']/li[8]").get_attribute('class')
#         if '_no' in heating:
#             heating = 0
#         else:
#             heating = 1

#         # 宽带
#         wifi = driver.find_element_by_xpath(
#             "//ul[@class='content__article__info2']/li[9]").get_attribute('class')
#         if '_no' in wifi:
#             wifi = 0
#         else:
#             wifi = 1

#         # 衣柜
#         closet = driver.find_element_by_xpath(
#             "//ul[@class='content__article__info2']/li[10]").get_attribute('class')
#         if '_no' in closet:
#             closet = 0
#         else:
#             closet = 1

#         # 天然气
#         natural_gas = driver.find_element_by_xpath(
#             "//ul[@class='content__article__info2']/li[11]").get_attribute('class')
#         if '_no' in natural_gas:
#             natural_gas = 0
#         else:
#             natural_gas = 1

        facility_info = get_facility_info(driver)
    
        community_info = get_community_info(driver)
    
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
        if len(driver.find_elements_by_xpath("//div[@class='content__article__info3 ']/ul/li/p")) > 2:
            house_description = ''
            _print("请调整子函数get_list_info的房源描述部分，有超过一条评论的情况需要全部考虑。（做成列表而不再是文本）", url)
        else:
            try:
                house_description = driver.find_element_by_xpath(
                    "//div[@class='content__article__info3 ']/ul/li/p[1]").text
            except:
                house_description = ''

                
        # 房源链接
        house_url = url

       

        # 每平米房价
        try:
            price_per_square_meter = round(price/content_article_info.get('area'), 2)
        except:
            _print('无法计算每平米房价：', url)
            price_per_square_meter = ''

        # 经纪人品牌
#         broker_brand = 'None'
#         try:
#             broker_brand = driver.find_element_by_xpath(
#                 "//div[@class='content__aside fr']/ul[@class='content__aside__list house-detail']/li/p").text
#         except:
#             _print('unale to locate broker element ', url)
#             # unable to locate element
#             pass

#         if ' 经纪人' in broker_brand:
#             broker_brand = broker_brand[:-4]
#         if '管家' in broker_brand:
#             broker_brand = broker_brand[:-3]

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
                       'house_url': house_url,
                        **community_info,
                       'price_per_square_meter': price_per_square_meter,
                       'missing_info':len(img_urls) < 2,
                       'version': os.environ.get('VER')
                       }
        return dict_single