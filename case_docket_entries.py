import asyncio
import pandas as pd
import aiohttp
import pickle

from configuration.config import Config
from services.api_services import ApiSession
from util import async_timed, get


config = Config()
api = ApiSession.instance()
api_base_url = config.base_api_url


@async_timed()
async def main(*target_caseids):
    async with aiohttp.ClientSession() as session:
        params = {'documents': 'false', 'docket_text': 'true'}
        headers = {'Authorization': f'Bearer {api.access_token}', 'Content-Type': 'application/json'}
        urls = [f'{api_base_url}/cases/entries/{caseid}' for caseid in target_caseids]
        requests = [get(session, url, params=params, headers=headers) for url in urls]
        dataframes = await asyncio.gather(*requests)
        with open('/Users/jwt/PycharmProjects/cpi_program/data_files/docket_entries.pkl', 'wb') as f:
            pickle.dump(dataframes, f, pickle.HIGHEST_PROTOCOL)


if __name__ == '__main__':
    df = pd.read_csv('/Users/jwt/PycharmProjects/cpi_program/data_files/civil_cases_2018-2022.csv')
    df[['Date Filed', 'Date Terminated', 'DateAgg']] = df[['Date Filed', 'Date Terminated', 'DateAgg']].apply(
        pd.to_datetime, yearfirst=True,
        dayfirst=False, errors='coerce')
    mask = df['IsProse'] == 'y'
    df1 = df[mask]
    target_caseids = df1['Case ID'].tolist()
    # caseids = [41091, 41099, 41106]
    asyncio.run(main(*target_caseids))
