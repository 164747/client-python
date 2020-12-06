import requests


class RESTClient:
    """ This is a custom generated class """
    DEFAULT_HOST = "api.polygon.io"

    def __init__(self, auth_key: str, timeout: int=None):
        self.auth_key = auth_key
        self.url = "https://" + self.DEFAULT_HOST

        self.session = requests.Session()
        self.session.params["apiKey"] = self.auth_key
        self.timeout = timeout

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def close(self):
        self.session.close()
