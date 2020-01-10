from selenium import webdriver

def find_next_button(driver:webdriver):
    '''
    下一页
    '''
    return driver.find_element_by_xpath("//div[@class='content__pg']/a[@class='next']")