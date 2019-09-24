import pandas as pd
import re
from bs4 import BeautifulSoup
from helper import _print
def get_info_of_single_url(self, driver, url):
    """
    get info of single url
    """
    driver.get(url)
    # TODO：支持对公寓房源的爬取。
    if 'apartment' in url:
        pass
    else:
        # 类型/标题：可能为空，导致标题的爬取报错
        # try:
        #     rent_type = driver.find_element_by_xpath("//p[@class='content__title']").text.split(' · ')[0]
        #     title = driver.find_element_by_xpath("//p[@class='content__title']").text.split(' · ')[1]
        # except:
        #     rent_type = '未知'
        #     title = driver.find_element_by_xpath("//p[@class='content__title']").text
        rent_type = driver.find_elements_by_xpath(
            "//p[@class='content__article__table']/span")[0].text
        title = driver.find_element_by_xpath(
            "//p[@class='content__title']").text

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
        # 信息卡照片：可能为空
        # TODO:是否有多个？有bug无法打开
#             try:
#                 duty_img = driver.find_element_by_xpath("//div[@class='content__subtitle']/ul/li/div/img").get_attribute('src').split('!')[0]
#             except:
#                 duty_img = ''

        # 信息卡号：可能为空
#             try:
#                 duty_id = driver.find_element_by_xpath("//div[@class='content__subtitle']/ul/li/div/p").text.split('证件号码：')[1].stripe()
#             except:
#                 duty_id = ''

        # 营业执照
        # TODO

        # 经纪备案：可能为空
        # TODO
        try:
            pass
        except:
            pass

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
            "//p[@class='content__aside--title']/span").text)

        # 特色标签列表
        tags = []
        for i in driver.find_elements_by_xpath("//p[@class='content__aside--tags']/i"):
            tags.append(i.text)
#             json_house_tags = json.dumps(json_house_tags, ensure_ascii=False)

        # 户型、面积、朝向
        house_type = driver.find_elements_by_xpath(
            "//p[@class='content__article__table']/span")[1].text
        area = int(driver.find_elements_by_xpath(
            "//p[@class='content__article__table']/span")[2].text.split('㎡')[0])
        orient = driver.find_elements_by_xpath(
            "//p[@class='content__article__table']/span")[3].text.split('朝')[1]

        # 经纪人姓名：可能为空
        # TODO

        # 经纪人联系方式
        try:
            broker_contact = driver.find_element_by_xpath(
                "//p[@class='content__aside__list--bottom oneline']").text
        except:
            broker_contact = ''

        # 最短租期/最长租期:统一显示为天数，一年360天(防止12个月和365天天数不相等的情况)，一个月30天
        rent_peroid = driver.find_element_by_xpath(
            "//div[@class='content__article__info']/ul/li[5]").text[3:]
        if '~' in rent_peroid:
            if '月' in rent_peroid:
                minimal_lease = int(rent_peroid.split('~')[0])*30
                maximal_lease = int(rent_peroid.split('~')[
                                    1].split('个月')[0])*30
            elif '年' in rent_peroid:
                minimal_lease = int(rent_peroid.split('~')[0])*360
                maximal_lease = int(rent_peroid.split('~')[
                                    1].split('年')[0])*360
            else:
                _print("请手工检查租期：", rent_peroid, url)
                minimal_lease = rent_peroid
                maximal_lease = rent_peroid
        else:
            minimal_lease = rent_peroid
            maximal_lease = rent_peroid

        # 所在楼层
        floor = driver.find_element_by_xpath(
            "//div[@class='content__article__info']/ul/li[8]").text[3:].split('/')[0]

        # 总楼层
        building_total_floors = int(driver.find_element_by_xpath(
            "//div[@class='content__article__info']/ul/li[8]").text[3:].split('/')[1].replace('层', ''))

        # 车位
        carport = driver.find_element_by_xpath(
            "//div[@class='content__article__info']/ul/li[11]").text[3:]

        # 用电
        electricity_type = driver.find_element_by_xpath(
            "//div[@class='content__article__info']/ul/li[14]").text[3:]

        # 入住
        check_in_date = driver.find_element_by_xpath(
            "//div[@class='content__article__info']/ul/li[3]").text[3:]

        # 看房
        reservation = driver.find_element_by_xpath(
            "//div[@class='content__article__info']/ul/li[6]").text[3:]

        # 电梯
        elevator = driver.find_element_by_xpath(
            "//div[@class='content__article__info']/ul/li[9]").text[3:]

        # 用水
        water = driver.find_element_by_xpath(
            "//div[@class='content__article__info']/ul/li[12]").text[3:]

        # 燃气
        # TODO:和下面的天然气有什么区别？
        gas = driver.find_element_by_xpath(
            "//div[@class='content__article__info']/ul/li[15]").text[3:]

        # 电视
        television = driver.find_element_by_xpath(
            "//ul[@class='content__article__info2']/li[2]").get_attribute('class')
        if '_no' in television:
            television = 0
        else:
            television = 1

        # 冰箱
        fridge = driver.find_element_by_xpath(
            "//ul[@class='content__article__info2']/li[3]").get_attribute('class')
        if '_no' in fridge:
            fridge = 0
        else:
            fridge = 1

        # 洗衣机
        washing_machine = driver.find_element_by_xpath(
            "//ul[@class='content__article__info2']/li[4]").get_attribute('class')
        if '_no' in washing_machine:
            washing_machine = 0
        else:
            washing_machine = 1

        # 空调
        air_condition = driver.find_element_by_xpath(
            "//ul[@class='content__article__info2']/li[5]").get_attribute('class')
        if '_no' in air_condition:
            air_condition = 0
        else:
            air_condition = 1

        # 热水器
        water_heater = driver.find_element_by_xpath(
            "//ul[@class='content__article__info2']/li[6]").get_attribute('class')
        if '_no' in water_heater:
            water_heater = 0
        else:
            water_heater = 1

        # 床
        bed = driver.find_element_by_xpath(
            "//ul[@class='content__article__info2']/li[7]").get_attribute('class')
        if '_no' in bed:
            bed = 0
        else:
            bed = 1

        # 暖气
        heating = driver.find_element_by_xpath(
            "//ul[@class='content__article__info2']/li[8]").get_attribute('class')
        if '_no' in heating:
            heating = 0
        else:
            heating = 1

        # 宽带
        wifi = driver.find_element_by_xpath(
            "//ul[@class='content__article__info2']/li[9]").get_attribute('class')
        if '_no' in wifi:
            wifi = 0
        else:
            wifi = 1

        # 衣柜
        closet = driver.find_element_by_xpath(
            "//ul[@class='content__article__info2']/li[10]").get_attribute('class')
        if '_no' in closet:
            closet = 0
        else:
            closet = 1

        # 天然气
        natural_gas = driver.find_element_by_xpath(
            "//ul[@class='content__article__info2']/li[11]").get_attribute('class')
        if '_no' in natural_gas:
            natural_gas = 0
        else:
            natural_gas = 1

        # 地址和交通，地铁便利性
        # TODO:地铁便利性的筛选标准，距任一地铁站的距离有小于1000m的
        subway_accessibility = 0
        try:
            list_subways = []
            for i in driver.find_elements_by_xpath("//div[@class='content__article__info4']/ul/li"):
                subway_line = i.text[3:].split(' - ')[0]
                subway_station = i.text.split(' - ')[1].split(' ')[0]
                subway_station_distance = int(i.text.split(
                    ' - ')[1].split(' ')[1].split('m')[0])
                list_subways.append(
                    [subway_line, subway_station, subway_station_distance])

                if subway_station_distance < 1000:
                    subway_accessibility = 1
            transportations = list_subways
        except:
            _print("地址和交通有未知错误：", url)
            transportations = ''

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
            _print("无小区最新成交信息：", url)
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

        # 每平米房价
        try:
            price_per_square_meter = round(price/area, 2)
        except:
            _print('无法计算每平米房价：', url)
            price_per_square_meter = ''

        # 经纪人品牌
        broker_brand = driver.find_element_by_xpath(
            "//div[@class='content__aside fr']/ul[@class='content__aside__list']/li/p").text
        if ' 经纪人' in broker_brand:
            broker_brand = broker_brand[:-4]
        if '管家' in broker_brand:
            broker_brand = broker_brand[:-3]

        # 上下楼便利性：无障碍性，楼层与电梯的合成项
        floor_accessibility = 0
        if elevator == '有':
            floor_accessibility = 1
        elif elevator == '无' and floor <= '3':
            floor_accessibility = 1
        elif elevator == 0 and floor == '低楼层':
            floor_accessibility = 1
        else:
            pass

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
                       'area': area,
                       'orient': orient,
                       'broker_name': '',
                       'broker_contact': broker_contact,
                       'minimal_lease': minimal_lease,
                       'maximal_lease': maximal_lease,
                       'floor': floor,
                       'building_total_floors': building_total_floors,
                       'carport': carport,
                       'electricity_type': electricity_type,
                       'check_in_date': check_in_date,
                       'reservation': reservation,
                       'elevator': elevator,
                       'water': water,
                       'gas': gas,
                       'television': television,
                       'fridge': fridge,
                       'washing_machine': washing_machine,
                       'air_condition': air_condition,
                       'water_heater': water_heater,
                       'bed': bed,
                       'heating': heating,
                       'wifi': wifi,
                       'closet': closet,
                       'natural_gas': natural_gas,
                       'transportations': transportations,
                       'community_deals': community_deals,
                       'house_description': house_description,
                       'house_url': house_url,
                       'city': city,
                       'district': district,
                       'bizcircle': bizcircle,
                       'community_name': community_name,
                       'community_url': community_url,
                       'price_per_square_meter': price_per_square_meter,
                       'broker_brand': broker_brand,
                       'floor_accessibility': floor_accessibility,
                       'subway_accessibility': subway_accessibility
                       }
        return dict_single