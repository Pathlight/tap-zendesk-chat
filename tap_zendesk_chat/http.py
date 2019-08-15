import requests
from singer import metrics
import backoff
import singer

BASE_URL = "https://www.zopim.com"

LOGGER = singer.get_logger()


class RateLimitException(Exception):
    pass


class Client(object):
    def __init__(self, config):
        # self.session = requests.Session()
        self.access_token = config["access_token"]
        self.user_agent = config.get("user_agent")
        self.session = requests.Session()

    @backoff.on_exception(backoff.expo,
                          RateLimitException,
                          max_tries=10,
                          factor=2)
    def request(self, endpoint, params={}, url=None, url_extra=""):
        with metrics.http_request_timer(endpoint) as timer:
            url = url or BASE_URL + "/api/v2/" + endpoint + url_extra
            headers = {"Authorization": "Bearer " + self.access_token}
            if self.user_agent:
                headers["User-Agent"] = self.user_agent
            request = requests.Request("GET", url, headers=headers, params=params)
            response = self.session.send(request.prepare())
            timer.tags[metrics.Tag.http_status_code] = response.status_code
        if response.status_code in [429, 502]:
            raise RateLimitException()
        try:
            response.raise_for_status()
        except:
            LOGGER.exception(response.content)
            raise
        return response.json()

    def incremental_request(self, endpoint, *args, **kwargs):
        return self.request("incremental/" + endpoint, *args, **kwargs)
