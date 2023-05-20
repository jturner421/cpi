import asyncio
import datetime
from pathlib import Path
import pandas as pd
import aiohttp
import pickle

from configuration.config import Config
from db.dbsession import get_postgres_db_session
from services.api_services import ApiSession, get_data
from services.db_services import get_reflected_tables
from services.case_services import get_civil_cases_by_date, create_event
from services.dataframe_services import create_merged_df, add_nos_grouping
from util import async_timed, get


config = Config()
api = ApiSession.instance()
get_postgres_db_session()
api_base_url = config.base_api_url


@async_timed()
async def main(*target_caseids):
    async with aiohttp.ClientSession() as session:
        params = {'documents': 'false', 'docket_text': 'true'}
        headers = {'Authorization': f'Bearer {api.access_token}', 'Content-Type': 'application/json'}
        urls = [f'{api_base_url}/cases/entries/{caseid}' for caseid in target_caseids]
        requests = [get(session, url, params=params, headers=headers) for url in urls]
        dataframes = await asyncio.gather(*requests)
        with open('data_files/green_belt_docket_entries.pkl', 'wb') as f:
            pickle.dump(dataframes, f, pickle.HIGHEST_PROTOCOL)


if __name__ == '__main__':
    stats_file = Path(Path.cwd() /'data_files'/ 'civil_cases_2020-2023.csv')
    if stats_file.is_file():
        stats = pd.read_csv(stats_file)
    else:
        nos, deadlines = get_reflected_tables()
        start_date = datetime.date(2020, 1, 1)
        end_date = datetime.date(2023, 5, 19)
        stats = get_civil_cases_by_date(start_date=start_date, end_date=end_date)
        stats = add_nos_grouping(stats, nos)
        stats.to_csv('/Users/jwt/PycharmProjects/cpi_program/data_files/civil_cases_2020-2023.csv',index=False)

    stats[['Date Filed', 'Date Terminated', 'DateAgg']] = stats[['Date Filed', 'Date Terminated', 'DateAgg']].apply(        pd.to_datetime, yearfirst=True,
        dayfirst=False, errors='coerce')
    mask = stats['IsProse'] == 'y'
    df1 = stats[mask]
    target_caseids = df1['Case ID'].tolist()
    # caseids = [41091, 41099, 41106]
    asyncio.run(main(*target_caseids))
