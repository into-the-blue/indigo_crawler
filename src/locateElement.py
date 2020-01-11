from selenium import webdriver

def find_next_button(driver:webdriver):
    '''
    下一页
    requirement:
    1. None
    '''
    return driver.find_element_by_xpath("//div[@class='content__pg']/a[@class='next']")

def find_elms_of_line(driver:webdriver):
    '''
    所有地铁线路
    requirement:
    1. 点击 `按地铁线`
    '''
    return driver.find_elements_by_xpath("//div[@class='filter__wrapper w1150']/ul[@data-target='station']/li[@data-type='line']")

def find_elms_of_stations(driver:webdriver):
    '''
    所有地铁站
    requirement:
    1. 打开地铁线路url
    '''
    return driver.find_elements_by_xpath("//div[@class='filter__wrapper w1150']/ul[@data-target='station']/li[@data-type='station']")