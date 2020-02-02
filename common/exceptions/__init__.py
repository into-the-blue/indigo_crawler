class UrlExistsException(Exception):
    def __init__(self, *args, **kwargs):
        super().__init__('Url already crawled', *args, **kwargs)


class ApartmentExpiredException(Exception):
    def __init__(self, *args, **kwargs):
        super().__init__('Apartment Expired', *args, **kwargs)


class ProxyBlockedException(Exception):
    def __init__(self, *args, **kwargs):
        super().__init__('Proxy blocked', *args, **kwargs)