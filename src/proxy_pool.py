import requests
import os
from selenium.webdriver.common.proxy import Proxy, ProxyType
from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from utils.util import _print
proxy_server = os.getenv('PROXY_SERVER')
chrome_driver_pth = os.getenv(
    'CHROME_DRIVER_PTH', '/Users/origami/Downloads/chromedriver')
is_ubuntu = os.getenv('PY_ENV', 'mac') == 'ubuntu'


def test_proxy(driver, proxy_url):
    try:
        driver.set_page_load_timeout(10)
        driver.get('https://sh.zu.ke.com/zufang')
        return True
    except:
        return False


def get_proxy():
    '''
    {'proxy': '175.43.156.61:9999',
    'fail_count': 0,
    'region': '',
    'type': '',
    'source': 'freeProxy07',
    'check_count': 2,
    'last_status': 1,
    'last_time': '2019-09-21 11:47:09'}
    '''
    return requests.get(f"http://{proxy_server}/get/").json()


def delete_proxy(proxy):
    requests.get(f"http://{proxy_server}/delete/?proxy={proxy}")


def init_driver():
    proxy_url = get_proxy().get('proxy')
    prox = Proxy()
    prox.proxy_type = ProxyType.MANUAL
    prox.http_proxy = proxy_url
    # prox.socks_proxy = "ip_addr:port"
    # prox.ssl_proxy = "ip_addr:port"
    capabilities = webdriver.DesiredCapabilities.CHROME
    prox.add_to_capabilities(capabilities)

    # options are unnecessary
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--disable-dev-shm-usage')
    if(is_ubuntu is True):
        driver = webdriver.Remote(
            'http://chrome:4444/wd/hub', desired_capabilities=capabilities, options=chrome_options)
        return driver, proxy_url
    else:
        driver = webdriver.Chrome(
            chrome_driver_pth, desired_capabilities=capabilities)
        return driver, proxy_url


def get_driver_with_proxy(times=0):
    if(times >= 10):
        _print('no available proxy')
        raise Exception('no available proxy')
    driver, proxy_url = init_driver()
    ok = test_proxy(driver, proxy_url)
    _print('PROXY', proxy_url, 'WORKS?', ok)
    if(ok):
        driver.maximize_window()
        return driver
    else:
        delete_proxy(proxy_url)
        driver.quit()
        return get_driver_with_proxy(times+1)
