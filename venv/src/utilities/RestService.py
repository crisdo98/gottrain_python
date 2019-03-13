import logging

import requests


class RestService:

    def __init__(self, username, password, authenticate_url):
        self.username = username
        self.password = password
        self.authenticate_url = authenticate_url
        self.logger = logging.getLogger("gottrain_logger")

    def get_token_header(self):
        payload = {"username": self.username, "password": self.password}
        r = requests.post(self.authenticate_url, data=payload)  # , verify=False)

        if r.status_code != requests.codes.ok:
            raise Exception("Failed to authenticate user: %s" % r.text)
        else:
            self.logger.info("Authenticated successfully....")

        r = r.json()

        token = r["token"]

        headers = {"X-Auth-Token": token}
        self.logger.debug(headers)
        return headers

    def requests_get(self, url, headers):
        r = requests.get(url, headers=headers)  # , verify=False)

        if r.status_code == 401:
            # re-authenticate credentials
            headers = self.get_token_header()
            self.requests_get(url, headers)
        elif r.status_code == 404:
            self.logger.error("Uri path does not exist")
            raise Exception("Uri path does not exist: %s" % r.text)
        elif r.status_code == 503:
            self.logger.error("Request interrupted")
            raise Exception("Requested interrupted: %s" % r.text)
        return r.content
