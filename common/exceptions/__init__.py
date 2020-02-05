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


class ValidatorInvalidValue(Exception):
    def __init__(self, *args, **kwargs):
        super().__init__('Invalid Value', *args, **kwargs)
