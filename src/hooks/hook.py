class Hook():
    def on_get_url(self, url, station_info=None):
        pass

    def on_get_apartment_info(self, info, location_info=None):
        pass

    def on_url_expired(self, url):
        pass


class HookHandler():
    def __init__(self, hooks=None):
        self.hooks = hooks or []

    def __call__(self, hook_name, *args):
        for hook in self.hooks:
            f = getattr(hook, hook_name)
            f and f(*args)
