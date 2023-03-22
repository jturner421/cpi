"""
Module that performs HTTP actions against a provided enpoint. Upon a succesful operation, json data is loaded into
a Pandas DataFrame and returned to the calling function.

"""

from typing import Dict
import httpx
import pandas as pd

from configuration.config import Config


class ApiSession:
    def __init__(self):
        self.access_token = ApiSession.get_api_token(self)
        self.url = Config.base_api_url
        self.headers = {'Authorization': f'Bearer {self.access_token}'}

    def get_api_token(self):
        """
        Get the API token from the ecfapi endpoint.
        """
        config = Config()
        url = config.token_url
        data = {'username': config.username, 'password': config.password}
        r = httpx.post(url, data=data, verify=False)
        return r.json()['access_token']

    def get(self, url: str, payload: Dict = None) -> pd.DataFrame:
        if payload:
            r = httpx.get(url, params=payload, headers=self.headers, verify=False, timeout=None).json()
        else:
            r = httpx.get(url).json()
        return pd.DataFrame(r['data'])

    def post(self, **kwargs):
        httpx.headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
        if kwargs['payload']:
            r = s.post(kwargs['url'], json=kwargs['payload']).json()
        else:
            r = s.get(kwargs['url']).json()
        return pd.DataFrame(r['data'])


