"""
Module that performs HTTP actions against a provided enpoint. Upon a succesful operation, json data is loaded into
a Pandas DataFrame and returned to the calling function.

"""

from typing import Dict
import httpx
import pandas as pd

from configuration.config import Config


class ApiSession:
    _instance = None

    def __init__(self):
        raise RuntimeError('Call instance() instead')

    @classmethod
    def instance(cls):
        if cls._instance is None:
            cls._instance = cls.__new__(cls)
            cls._instance.access_token = ApiSession._get_api_token(cls)
            cls._instance.url = Config.base_api_url
            cls._instance.headers = {'Authorization': f'Bearer {cls._instance.access_token}'}
        return cls._instance

    def _get_api_token(self) -> str:
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


def get_data(case_ids, access_token, url, params, event, overall_type) -> pd.DataFrame:
    """
   Retrieves data from the API for a list of case IDs. Makes distinction between specific event and
   overall catagory type.

   :param case_ids: list of case IDs
   :param access_token: API access token
   :param url: API endpoint
   :param params: API parameters
   :param event: event type e.g. ['cmp','cmp'] for a complaint
   :param overall_type: overall type e.g. 'motion' for all motions regardless of type
   :return: dataframe of data

    """
    headers = {'Authorization': f'Bearer {access_token}', 'Content-Type': 'application/json'}
    if event:
        data = {'case_ids': case_ids, 'subtype': event}
    if overall_type:
        data = {'case_ids': case_ids, 'overall_type': overall_type}
    r = httpx.post(url, params=params, headers=headers, json=data, verify=False, timeout=None)
    df = pd.DataFrame(r.json()['data'])

    return df
