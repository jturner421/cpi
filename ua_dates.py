import asyncio
import datetime
import pandas as pd
import numpy as np
import json
import httpx
import aiohttp
from aiohttp import ClientSession

from configuration.config import Config
from services.api_services import ApiSession
from util import async_timed, get

from colorama import Fore, Style




@async_timed()
async def get_docket_entries_for_case(caseid):
    while True:
        # caseid = await q.get()
        print(Fore.YELLOW + f'Getting docket entries for case {caseid}...', flush=True)
        url = f'{api_base_url}/cases/entries/{caseid}'
        headers = {'Authorization': f'Bearer {api.access_token}', 'Content-Type': 'application/json'}
        params = {'documents': 'false', 'docket_text': 'true'}
        async with httpx.AsyncClient() as client:
            r = await client.get(url, params=params, headers=headers, timeout=None)
        # df = pd.DataFrame(r.json()['data'])
        # df.sort_values(by=['de_seqno'], ascending=True, inplace=True)
        # await q.put((caseid, df))
    # return df


@async_timed()
async def main(*caseids):
    config = Config()
    api = ApiSession()
    api_base_url = config.base_api_url
    async with aiohttp.ClientSession() as session:
        params = {'documents': 'false', 'docket_text': 'true'}
        headers = {'Authorization': f'Bearer {api.access_token}', 'Content-Type': 'application/json'}
        urls = [f'{api_base_url}/cases/entries/{caseid}' for caseid in caseids]
        requests = [get(session, url, params=params, headers=headers) for url in urls]
        status_codes = await asyncio.gather(*requests)
        print(status_codes)


if __name__ == '__main__':
    import time

    df = pd.read_csv('/Users/jwt/PycharmProjects/dashboard/CPI/prose_merged.csv')
    df[['Date Filed', 'Date Terminated', 'DateAgg']] = df[['Date Filed', 'Date Terminated', 'DateAgg']].apply(
        pd.to_datetime, yearfirst=True,
        dayfirst=False, errors='coerce')
    mask = df['Date Filed'] <= datetime.datetime(2018, 2, 1)
    df1 = df[mask]
    caseids = df1['Case ID'].tolist()
    # caseids = [41091, 41099, 41106]
    asyncio.run(main(*caseids))
