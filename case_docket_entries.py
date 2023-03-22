import asyncio
import datetime
import pandas as pd
import numpy as np
import json
import httpx
import aiohttp
from aiohttp import ClientSession
import pickle

from configuration.config import Config
from services.api_services import ApiSession
from util import async_timed, get, get_httpx

from colorama import Fore, Style

config = Config()
api = ApiSession()
api_base_url = config.base_api_url


@async_timed()
async def main(*caseids):
    async with aiohttp.ClientSession() as session:
        params = {'documents': 'false', 'docket_text': 'true'}
        headers = {'Authorization': f'Bearer {api.access_token}', 'Content-Type': 'application/json'}
        urls = [f'{api_base_url}/cases/entries/{caseid}' for caseid in caseids]
        requests = [get(session, url, params=params, headers=headers) for url in urls]
        dataframes = await asyncio.gather(*requests)
        with open('dataframes.pkl', 'wb') as f:
            pickle.dump(dataframes, f, pickle.HIGHEST_PROTOCOL)


if __name__ == '__main__':
    import time

    df = pd.read_csv('/Users/jwt/PycharmProjects/dashboard/CPI/prose_merged.csv')
    df[['Date Filed', 'Date Terminated', 'DateAgg']] = df[['Date Filed', 'Date Terminated', 'DateAgg']].apply(
        pd.to_datetime, yearfirst=True,
        dayfirst=False, errors='coerce')
    mask = df['Date Filed'] <= datetime.datetime(2018, 12, 31)
    df1 = df[mask]
    caseids = df1['Case ID'].tolist()
    # caseids = [41091, 41099, 41106]
    asyncio.run(main(*caseids))
