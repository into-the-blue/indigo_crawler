import requests
import os
from selenium.webdriver.common.proxy import Proxy, ProxyType
from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.common.exceptions import InvalidSessionIdException, TimeoutException, SessionNotCreatedException, WebDriverException
from ..exceptions import TooManyTimesException, NoProxyAvailableException
from ..utils.logger import logger
from ..db import db

proxy_server = os.getenv('PROXY_SERVER')


is_ubuntu = os.getenv('PY_ENV', 'mac') == 'ubuntu'
TEST_URL = 'https://sh.zu.ke.com/zufang'


def test_proxy(driver, test_url=None):
    test_url = test_url or TEST_URL
    try:
        driver.set_page_load_timeout(20)
        driver.get(test_url)
        return True
    except TimeoutException:
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
    res = requests.get(f"{proxy_server}/get/").json()
    if res.get('code') == 0:
        raise NoProxyAvailableException()
    return res


# def delete_proxy(proxy=None):
#     _proxy = proxy if bool(proxy) else None
#     if not bool(_proxy):
#         return
#     requests.get(f"{proxy_server}/delete/?proxy={proxy}")


def connect_to_driver():
    capabilities = webdriver.DesiredCapabilities.CHROME

    # options are unnecessary
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--no-sandbox')

    logger.info('connecting remote webdriver')
    driver = webdriver.Remote(
        'http://chrome:4444/wd/hub', desired_capabilities=capabilities, options=chrome_options)
    # clean cookies
    driver.delete_all_cookies()
    driver.maximize_window()
    logger.info('driver inited')
    return driver


def create_capabilities_with_proxy(proxy_url: str):
    prox = Proxy()
    prox.proxy_type = ProxyType.MANUAL
    prox.http_proxy = proxy_url

    capabilities = webdriver.DesiredCapabilities.CHROME
    prox.add_to_capabilities(capabilities)
    return capabilities

# def start_new_session(driver:webdriver, capabilities):
#     driver.start_session(capabilities=capabilities)


def setup_proxy_for_driver(driver: webdriver, test_url=None, times=0):
    if times > 9:
        logger.warning('setup_proxy_for_driver no available proxy')
        raise TooManyTimesException('setup_proxy_for_driver')
    try:
        try:
            driver.quit()
        except:
            pass
        proxy_url = get_proxy().get('proxy')

        logger.info('proxy get {}'.format(proxy_url))

        capabilities = create_capabilities_with_proxy(proxy_url)

        logger.info('start new session')
        driver.start_session(capabilities=capabilities)
        logger.info('start testing proxy')

        ok = test_proxy(driver, test_url)
        if not ok:
            logger.warning(
                'proxy checking failed for {} times'.format(times+1))
            return setup_proxy_for_driver(driver, test_url, times=times+1)
        logger.info('proxy works')

        return driver

    except SessionNotCreatedException:
        logger.error('Failed to start a new session')
        return setup_proxy_for_driver(connect_to_driver(), test_url, times=times+1)

    except InvalidSessionIdException as e2:
        logger.error('Session id invalid {}'.format(e2))
        return setup_proxy_for_driver(driver, test_url, times=times+1)

    except WebDriverException as e3:
        logger.error('No active session with ID')
        return setup_proxy_for_driver(driver, test_url, times=times+1)

    except NoProxyAvailableException:
        logger.error('No proxy')
        db._report_error({
            'error_source': 'proxy',
            'message': 'no_proxy_available',
            'url': None,
            'payload': {
            }
        })
        return setup_proxy_for_driver(driver, test_url, times=times+1)

    except Exception as e:
        logger.error(f'setup_proxy_for_driver {e}')
        raise e


# def renew_or_create_driver(test_url=None):
#     '''
#     shouldn't use global driver, remove it in the future
#     '''
#     global global_driver
#     driver = global_driver or init_driver()
#     return setup_proxy_for_driver(driver, test_url)
