from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from ..utils.logger import logger
from ..db import db


def noSuchElement(method_name: str):
    def _internal(func):
        def __internal(driver: webdriver, path: str, no_empty: bool = True, report=True, *args, **kwargs):
            try:
                elms = func(driver, path, *args, **kwargs)
                if method_name == 'find_elements_by_xpath' and no_empty and len(elms) == 0:
                    raise NoSuchElementException('No element found')
                return elms
            except NoSuchElementException as e:
                if report:
                    # save intb db
                    url = driver.current_url
                    page_source = driver.page_source
                    logger.error('{},{},{}'.format(
                        method_name, path, driver.current_url))
                    db.report_no_such_elm_error(
                        method_name, path, url, page_source)
                raise e
        return __internal
    return _internal


@noSuchElement('find_elements_by_xpath')
def find_elms_by_xpath(driver: webdriver, xpath: str, no_empty: bool = True, report: bool = True):
    return driver.find_elements_by_xpath(xpath)


@noSuchElement('find_element_by_xpath')
def find_elm_by_xpath(driver: webdriver, xpath: str, report: bool = True):
    return driver.find_element_by_xpath(xpath)


@noSuchElement('find_element_by_link_text')
def find_elm_by_link_text(driver: webdriver, text: str):
    return driver.find_element_by_link_text(text)


def find_next_button(driver: webdriver):
    '''
    下一页
    可能找不到
    requirement:
    1. 列表页面
    '''
    return find_elm_by_xpath(driver, "//div[@class='content__pg']/a[@class='next']", report=False)


def find_elms_of_line(driver: webdriver):
    '''
    所有地铁线路
    requirement:
    1. 点击 `按地铁线`
    '''
    return find_elms_by_xpath("//div[@class='filter__wrapper w1150']/ul[@data-target='station']/li[@data-type='line']")


def find_elms_of_stations(driver: webdriver):
    '''
    所有地铁站
    requirement:
    1. 点击地铁线路
    '''
    return find_elms_by_xpath(driver, "//div[@class='filter__wrapper w1150']/ul[@data-target='station']/li[@data-type='station']")


def find_paging_elm(driver: webdriver):
    '''
    页码容器
    requirement:
    1. 列表页面
    '''
    return find_elm_by_xpath(driver, "//div[@class='content__pg']")


def find_paging_elm_index(driver: webdriver, index: int):
    '''
    当前页面
    requirement:
    1. 列表页面
    '''
    return find_elm_by_xpath(driver, "//div[@class='content__pg']/a[@data-page={}]".format(index))


def find_apartments_in_list(driver: webdriver):
    '''
    列表中的房子
    requirement:
    1. 列表页面
    '''
    return find_elms_by_xpath(driver, "//a[@class='content__list--item--aside']", no_empty=False)


def find_elm_of_latest_btn(driver: webdriver):
    '''
    最新上架按钮
    requirement:
    1. 列表页面
    '''
    return find_elm_by_link_text(driver, '最新上架')


def find_elm_of_house_type(driver: webdriver, index: int):
    '''
    house type, 面积, 朝向
    index: 1, 2
    requirement:
    1. 详情页面
    '''
    elms = find_elms_by_xpath(driver, "//ul[@class='content__aside__list']/li")
    return elms[index].text.replace(
        elms[index].find_element_by_xpath('./span').text, '')


def find_elm_of_basic_detail(driver: webdriver):
    '''
    基本信息

    requirement:
    1. 详情页面
    '''
    return find_elms_by_xpath(driver,
                              "//div[@class='content__article__info']/ul/li")


def find_elm_of_facility_detail(driver: webdriver):
    '''
    基础设施

    requirement:
    1. 详情页面
    '''
    return find_elms_by_xpath(driver,
                              "//ul[@class='content__article__info2']/li")


def find_city_elm(driver: webdriver):
    '''
    城市名字
    可能为空
    requirement:
    1. 详情页面
    '''
    return find_elm_by_xpath(driver,
                             "//div[@class='bread__nav w1150 bread__nav--bottom']/p/a[1]")


def find_district_elm(driver: webdriver):
    '''
    城区

    requirement:
    1. 详情页面
    '''
    return find_elm_by_xpath(driver,
                             "//div[@class='bread__nav w1150 bread__nav--bottom']/p/a[2]")


def find_bizcircle_elm(driver: webdriver):
    '''
    商圈

    requirement:
    1. 详情页面
    '''
    return find_elm_by_xpath(driver,
                             "//div[@class='bread__nav w1150 bread__nav--bottom']/p/a[3]")


def find_community_elm(driver: webdriver):
    '''
    小区

    requirement:
    1. 详情页面
    '''
    return find_elm_by_xpath(driver,
                             "//div[@class='bread__nav w1150 bread__nav--bottom']/h1/a")


def locate_elm_of_offline_text(driver: webdriver):
    '''
    过期文字
    requirement:
    1. 详情页面
    '''
    return find_elm_by_xpath(driver,
                             "//p[@class='offline__title']", report=False)


def locate_apartment_title(driver):
    '''
    标题
    requirement:
    1. 详情页面
    '''
    return find_elm_by_xpath(driver,
                             "//p[@class='content__title']")


def locate_updated_at(driver):
    '''
    上次维护时间
    requirement:
    1. 详情页面
    '''
    return find_elm_by_xpath(driver, "//div[@class='content__subtitle']")


def locate_house_code(driver):
    '''
    房源编码
    requirement:
    1. 详情页面
    '''
    return find_elm_by_xpath(driver,
                             "//div[@class='content__subtitle']/i[@class='house_code']")


def locate_img_list(driver):
    '''
    图片
    requirement:
    1. 详情页面
    '''
    return find_elms_by_xpath(driver, "//div[@class='content__thumb--box']/ul[@class='content__article__slide--small content__article__slide_dot']/li/img")


def locate_price(driver):
    '''
    价格
    requirement:
    1. 详情页面
    '''
    return find_elm_by_xpath(driver,
                             "//div[@class='content__aside--title']/span")


def locate_tags(driver):
    '''
    可能为null
    标签
    requirement:
    1. 详情页面
    '''
    return find_elms_by_xpath(driver, "//p[@class='content__aside--tags']/i", report=False)


def locate_house_desc(driver):
    '''
    房源描述
    requirement:
    1. 详情页面
    '''
    return find_elm_by_xpath(driver, "//div[@class='content__article__info3']/p[@data-el='houseComment']", report=False)


def locate_transportation(driver):
    '''
    交通信息
    requirement:
    1. 详情页面
    '''
    return find_elms_by_xpath(driver,
                              "//div[@class='content__article__info4 w1150']/ul/li", no_empty=False)


def locate_num_of_apartment(driver):
    '''
    房屋总数
    requirement:
    1. 列表页面
    '''
    return find_elm_by_xpath(driver,
                             "//p[@class='content__title']/span[@class='content__title--hl']")


def get_num_of_apartment(driver):
    '''
    房屋总数
    requirement:
    1. 列表页面
    '''
    return int(locate_num_of_apartment(driver).text)


list_page = [
    {'func': find_next_button, 'args': []},
    {'func': find_paging_elm, 'args': []},
    {'func': find_paging_elm_index, 'args': [2]},
    {'func': find_apartments_in_list, 'args': []},
    {'func': find_elm_of_latest_btn, 'args': []},
    {'func': locate_num_of_apartment, 'args': []}
]

detail_page = [
    {'func': find_elm_of_house_type, 'args': [1]},
    {'func': find_elm_of_house_type, 'args': [2]},
    {'func': find_elm_of_basic_detail, 'args': []},
    {'func': find_elm_of_facility_detail, 'args': []},
    {'func': find_city_elm, 'args': []},
    {'func': find_district_elm, 'args': []},
    {'func': find_bizcircle_elm, 'args': []},
    {'func': find_community_elm, 'args': []},
    {'func': locate_elm_of_offline_text, 'args': []},
    {'func': locate_apartment_title, 'args': []},
    {'func': locate_updated_at, 'args': []},
    {'func': locate_house_code, 'args': []},
    {'func': locate_img_list, 'args': []},
    {'func': locate_tags, 'args': []},
    {'func': locate_house_desc, 'args': []},
    {'func': locate_transportation, 'args': []},
]
