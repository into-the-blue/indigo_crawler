class UrlExistsException(Exception):
    def __init__(self, *args, **kwargs):
        super().__init__('Url already crawled', *args, **kwargs)


class ApartmentExpiredException(Exception):
    def __init__(self, *args, **kwargs):
        super().__init__('Apartment Expired', *args, **kwargs)


class ProxyBlockedException(Exception):
    def __init__(self, *args, **kwargs):
        super().__init__('Proxy blocked', *args, **kwargs)


class ElementNotFoundException(Exception):
    def __init__(self, *args, **kwargs):
        super().__init__('Element not found', *args, **kwargs)


class NoTaskException(Exception):
    def __init__(self, *args, **kwargs):
        super().__init__('No Task', *args, **kwargs)


class ValidatorInvalidValueException(Exception):
    def __init__(self, *args, **kwargs):
        super().__init__('Invalid Value', *args, **kwargs)


class UrlCrawlerNoMoreNewUrlsException(Exception):
    def __init__(self, *args, **kwargs):
        super().__init__('No more new urls', *args, **kwargs)

class TooManyTimesException(Exception):
    def __init__(self, *args, **kwargs):
        super().__init__('Too many times', *args, **kwargs)