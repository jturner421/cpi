import httpx

from configuration.config import Config


def get_api_token():
    """
    Get the API token from the ecfapi endpoint.
    """
    config = Config()
    url = config.token_url
    data = {'username': config.username, 'password': config.password}
    r = httpx.post(url, data=data, verify=False)
    return r.json()['access_token']



