import requests
import os
from selenium.webdriver.common.proxy import Proxy, ProxyType
from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.common.exceptions import InvalidSessionIdException, TimeoutException, SessionNotCreatedException
from utils.util import logger
proxy_server = os.getenv('PROXY_SERVER')
chrome_driver_pth = os.getenv(
    'CHROME_DRIVER_PTH', '/Users/origami/Downloads/chromedriver')
is_ubuntu = os.getenv('PY_ENV', 'mac') == 'ubuntu'
TEST_URL = 'https://sh.zu.ke.com/zufang'


global_driver = None


def test_proxy(driver, test_url=None):
    test_url = test_url or TEST_URL
    try:
        driver.set_page_load_timeout(10)
        driver.get(test_url)
        return True
    except TimeoutException:
        return False
    except Exception as e:
        logger.error('test_proxy', e)
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
    return requests.get(f"{proxy_server}/get/").json()


active_proxy = None


def delete_proxy(proxy=None):
    _proxy = proxy if bool(proxy) else active_proxy
    if not bool(_proxy):
        return
    requests.get(f"{proxy_server}/delete/?proxy={proxy}")


def init_driver():
    global global_driver
    global_driver = None
    capabilities = webdriver.DesiredCapabilities.CHROME

    # options are unnecessary
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--no-sandbox')

    if is_ubuntu:
        logger.info('connecting remote webdriver')
        driver = webdriver.Remote(
            'http://chrome:4444/wd/hub', desired_capabilities=capabilities, options=chrome_options)
    else:
        driver = webdriver.Chrome(
            chrome_driver_pth, desired_capabilities=capabilities, options=chrome_options)
        # clean cookies
    driver.delete_all_cookies()
    driver.maximize_window()
    global_driver = driver
    logger.info('driver inited')
    return driver


def setup_proxy_for_driver(driver: webdriver, test_url=None, times=0,):
    if times > 9:
        logger.warning('setup_proxy_for_driver no available proxy')
        raise Exception('setup_proxy_for_driver', 'no available proxy')
    try:
        proxy_url = get_proxy().get('proxy')
        logger.info('proxy get {}'.format(proxy_url))
        prox = Proxy()
        prox.proxy_type = ProxyType.MANUAL
        prox.http_proxy = proxy_url

        capabilities = webdriver.DesiredCapabilities.CHROME
        prox.add_to_capabilities(capabilities)
        logger.info('start new session')
        try:
            driver.quit()
        except:
            pass
        driver.start_session(capabilities=capabilities)
        logger.info('start testing proxy')
        ok = test_proxy(driver, test_url)
        if not ok:
            logger.warning(
                'proxy checking failed for {} times'.format(times+1))
            return setup_proxy_for_driver(driver, test_url, times=times+1)
        logger.info('proxy works')

        global active_proxy
        active_proxy = proxy_url

        return driver

    except SessionNotCreatedException:
        logger.error('Failed to start a new session')
        return setup_proxy_for_driver(init_driver(), test_url, times=times+1)

    except InvalidSessionIdException as e2:
        logger.error('Session id invalid {}'.format(e2))
        return setup_proxy_for_driver(driver, test_url, times=times+1)

    except Exception as e:
        logger.error(f'setup_proxy_for_driver {e}')
        raise e


def renew_or_create_driver(test_url=None):
    global global_driver
    driver = global_driver or init_driver()
    return setup_proxy_for_driver(driver, test_url)


# def get_driver_with_proxy(times=0, test_url=None):
#     test_url = test_url or TEST_URL
#     if times >= 10:
#         logger.warning('no available proxy')
#         raise Exception('no available proxy')
#     driver, proxy_url = init_driver()
#     ok = test_proxy(driver, test_url)
#     logger.info('PROXY', proxy_url, 'WORKS?', ok)
#     if ok:
#         active_proxy = proxy_url
#         return driver
#     else:
#         # delete_proxy(proxy_url)
#         driver.quit()
#         return get_driver_with_proxy(times+1)
