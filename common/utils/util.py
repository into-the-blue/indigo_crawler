import re

def extract_house_id(house_code):
    return re.findall('[0-9]+', house_code)[0]


def extract_house_code_from_url(url):
    return re.findall('(?<=\/)[a-zA-Z]+[0-9]+(?=\.)', url)[0]


def normal_msg(args):
    return ' '.join(map(lambda x: str(x), args))




def currentDate():
    tz = timezone(timedelta(hours=8))
    return datetime.now(tz=tz).strftime('%Y-%m-%d %H:%M:%S')


def find_index(func, li, first=True):
    res = [idx for idx, item in enumerate(li) if bool(func(item))]
    return res[0] if len(res) > 0 else -1


def find_item(func, li, first=True):
    res = [item for idx, item in enumerate(li) if bool(func(item))]
    return res[0] if len(res) > 0 else None


def cleanNoneValue(arr):
    return list(filter(lambda x: bool(x), arr))

