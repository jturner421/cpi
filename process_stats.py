import asyncio
import random
import os
from multiprocessing import current_process
from threading import current_thread
import datetime
import multiprocessing
import pandas as pd
import numpy as np
import datetime
import json
import aiohttp
from aiohttp import ClientSession

from configuration.config import Config
from services.api_services import ApiSession
from util import async_timed


from colorama import Fore, Style

config = Config()
api = ApiSession()
api_base_url = config.base_api_url

async def rnd_sleep(t):
    # sleep for T seconds on average
    await asyncio.sleep(t * random.random() * 2)


def find_ua_date(docnum, ua):
    for index, row in ua.iterrows():
        if docnum in row['dt_text']:
            ua_date = row['de_date_filed']
            return ua_date
            break
        else:
            ua_date = np.nan
    return ua_date


async def get_ua_date(q: asyncio.Queue) -> pd.DataFrame:
    """
    Get under advisement date for a case based on the document number provided in the dataframe row.  Applied as a lambda
    function to a dataframe. Example:

    df['Under Advisement Date'] = df.apply(lambda x: get_ua_date(x['Case ID'], x['IFP Document Number'], ua), axis=1)

    The case id is used to subset the under advisement dataframe to only the rows for the case id in question.  The document
    number is used as a search term to find the under advisement date in the dt_text column.

    :param case_id: case id for the case
    :param doc_num: document number for the filed document
    :param df: dataframe containing under advisement data
    """
    while True:
        case_dates = []
        case_id, target = await q.get()
        print(Fore.BLUE + f'Getting ua dates for {case_id}...', flush=True)
        case_dates.append({'case_id': case_id})
        st = 'forma pauperis'
        target['de_type'] = target['de_type'].str.strip()
        target['dp_type'] = target['dp_type'].str.strip()
        target['dp_sub_type'] = target['dp_sub_type'].str.strip()

        # locate complaints
        case_dates = await _find_complaint(case_dates, target)
        if case_dates[1]['cmp_date']:
            # check for ifp motion
            await _get_ifp_date(case_dates, target)
            await _get_screening_date(case_dates, target)
            # Get all under advisement entries
            mask = target['dp_sub_type'] == 'madv'
            ua = target[mask]

            if not ua.empty:
                # Find the under advisement date for the complaint
                await _get_ua_date(case_dates, ua)
            else:
                await _check_for_trust_fund_statement(case_dates, target)
        print(Fore.WHITE + f'Finished getting case dates for: {case_id}')
        await rnd_sleep(.3)
        q.task_done()
        return case_dates

        # return case_dates
        # if ua.empty:
        #     # Was there a voluntary dismissal or termination due to no prisoner trust fund statement?
        #     s1 = target.loc[target['dp_sub_type'] == 'voldism']
        #     s2 = target.loc[target['dp_sub_type'] == 'termcs']
        #     s3 = target.loc[target['dp_sub_type'] == 'prose2']
        #     dism = pd.concat([s1, s2, s3])
        #     # take the last date filed
        #     if not dism.empty:
        #         dis_date = dism['de_date_filed'].iloc[-1]
        #     case_dates.append({'dis_date': dis_date})

        #
        # # was an amended complaint filed?
        # mask = target['dp_sub_type'] == 'pamdcmp'
        # amdcmp = target[mask]
        # if not amdcmp.empty:
        #     cmp_docnum = amdcmp['de_document_num'].iloc[0].astype(int)
        # cmp_docnum = f'[{int(cmp_docnum)}]'
        # amdcmp_date = amdcmp['de_date_filed'].iloc[0]
        #

        #

        #

        #

        #
        #
        #     # return case_dates
        #
        # # check for SS case
        # s1 = target.loc[target['dp_sub_type'] == 'miscord']
        # if not s1.empty:
        #     ltp_date = s1['de_date_filed'].iloc[0]
        # case_dates.append({'ltp_date': ltp_date})
        # target_str = 'Prepaying Fees or Costs'
        # res = target.loc[target['dt_text'].str.contains(target_str)]
        # if not res.empty:
        #     ua_date = res['de_date_filed'].iloc[-1]
        # case_dates.append({'ua_date': ua_date})
        # # return case_dates
        # else:
        # ua_date = np.nan
        # ltp_date = np.nan
        # case_dates.append({'ua_date': ua_date, 'ltp_date': ltp_date})
        # # return case_dates
        #
        # if ua_date:
        #     # get all leave to proceed orders
        #     try:
        #         mask = target['dp_sub_type'] == 'leave'
        #         ltp = target[mask]
        #         ltp = ltp.drop_duplicates(subset=['de_document_num'], keep='first')
        #         ltp_date = ltp['de_date_filed'].iloc[0]
        #         case_dates.append({'ltp_date': ltp_date})
        #     except IndexError:
        #         ltp_date = np.nan
        #         case_dates.append({'ltp_date': ltp_date})


async def _check_for_trust_fund_statement(case_dates, target):
    # Was the case dimissed due to lack or prisoner trust fund statement?
    mask = target['dp_sub_type'] == 'termpscs'
    term = target[mask]
    if not term.empty:
        term_date = term['de_date_filed'].iloc[0]
        case_dates.append({'tfs_date': term_date})
    else:
        case_dates.append({'tfs_date': np.nan})
    return case_dates


async def _get_ua_date(case_dates, ua):
    if not ua.empty:
        ua_date = find_ua_date(case_dates[1]['cmp_docnum'], ua)
    # check for alternative ua dates
    if ua_date is np.nan and case_dates[2]['ifp_date'] is not np.nan:
        # use IFP motion date
        ua_date = find_ua_date(case_dates[2]['ifp_docnum'], ua)
    elif ua_date is np.nan and case_dates[3]['screening_date'] is not np.nan:
        # use screening motion date
        target_text = 'Screening'
        for index, row in ua.iterrows():
            if target_text in row['dt_text']:
                ua_date = row['de_date_filed']
                case_dates.append({'ua_date': ua_date})
                break
    else:
        case_dates.append({'ua_date': ua_date})
    return case_dates


async def _get_screening_date(case_dates, target):
    # Check for dummy screening motion
    mask = target['dp_sub_type'] == 'dummyscr'
    screening = target[mask]
    if not screening.empty:
        screening_docnum = screening['de_document_num'].iloc[0].astype(int)
        screening_docnum = f'[{int(screening_docnum)}]'
        screening_date = screening['de_date_filed'].iloc[0]
        case_dates[1]['cmp_date'] = screening_date
        case_dates[1]['cmp_docnum'] = screening_docnum
    else:
        screening_docnum = '0'
        screening_date = np.nan
    case_dates.append({'screening_date': screening_date, 'screening_docnum': screening_docnum})
    return case_dates


async def _get_ifp_date(case_dates, target):
    mask = target['dp_sub_type'] == 'ifp'
    ifp = target[mask]
    if not ifp.empty:
        ifp_docnum = ifp['de_document_num'].iloc[0].astype(int)
        ifp_docnum = f'[{int(ifp_docnum)}]'
        ifp_date = ifp['de_date_filed'].iloc[0]
    else:
        ifp_docnum = '0'
        ifp_date = np.nan
    case_dates.append({'ifp_date': ifp_date, 'ifp_docnum': ifp_docnum})
    return case_dates


async def _find_complaint(case_dates, target):
    mask = target['de_type'] == 'cmp'
    cmp = target[mask]
    if not cmp.empty:
        cmp_docnum = cmp['de_document_num'].iloc[0].astype(int)
        cmp_docnum = f'[{int(cmp_docnum)}]'
        cmp_date = cmp['de_date_filed'].iloc[0]
        case_dates.append({'cmp_date': cmp_date, 'cmp_docnum': cmp_docnum})
    else:
        cmp_docnum = np.nan
        cmp_date = np.nan
        case_dates.append({'cmp_date': cmp_date, 'cmp_docnum': cmp_docnum})
    return case_dates


def find_ua_date(docnum, ua):
    matches = [docnum, "Screening", 'Complaint', 'forma pauperis', 'Habeas']
    for index, row in ua.iterrows():
        a_string = row['dt_text']
        if any(x in a_string for x in matches):
            ua_date = row['de_date_filed']
            break
        else:
            ua_date = np.nan
    return ua_date


def create_merged_df(original_df, candidate_df) -> pd.DataFrame:
    df = pd.merge(original_df, candidate_df, left_on='Case ID', right_on='de_caseid', how='left')
    df.dropna(subset=['de_caseid'], inplace=True)
    df = df.drop(
        ['Cause of Action', 'Diversity Defendant', 'Diversity Plaintiff', 'IsProse', 'de_caseid',
         'de_date_enter', 'de_who_entered', 'initials', 'name', 'pr_type', 'pr_type', 'pr_crttype'], axis=1)
    df.drop_duplicates(subset=['Case ID'], keep='first', inplace=True)
    df['de_document_num'] = df['de_document_num'].fillna(0).astype(int)
    df['de_seqno'] = df['de_seqno'].fillna(0).astype(int)
    df['dp_seqno'] = df['dp_seqno'].fillna(0).astype(int)
    df = df.rename(columns={'de_seqno': 'DE SeqNum',
                            'dp_seqno': 'DP SeqNum',
                            'de_document_num': 'DocNum',
                            'dp_type': 'DP Type',
                            'dp_sub_type': 'DP SubType',
                            'de_date_filed': 'Date Filed'})
    return df





@async_timed()
async def main(*caseids):
    # q = asyncio.Queue()
    # await asyncio.gather(*(chain(caseid) for caseid in caseids))
    # put the case ids in the queue
    # for caseid in caseids:
    #     q.put_nowait(caseid)
    started_at = time.monotonic()
    producers = [asyncio.create_task(get_docket_entries_for_case(caseid, q)) for caseid in caseids]
    await asyncio.gather(*producers)
    await q.join()
    total_time = time.monotonic() - started_at
    print(Fore.GREEN + f'Finished in {total_time:.2f} seconds', flush=True)

    # task1 = [asyncio.create_task(get_docket_entries_for_case(caseid, q)) for caseid in caseids]
    # task2 = [asyncio.create_task(get_ua_date(q)) for caseid in caseids]
    # a, b = loop.run_until_complete(asyncio.gather(*task1, *task2))

    # await q.join()
    # for t in task2:
    #     t.cancel()

    # df2 = pd.json_normalize(target_dates)
    # df2.columns = ['Case ID', 'ua_date', 'ltp_date', 'dis_date']
    #
    # ua = pd.read_csv('/Users/jwt/PycharmProjects/dashboard/CPI/ua.csv')
    # ua['de_date_filed'] = ua['de_date_filed'].apply(pd.to_datetime, yearfirst=True, dayfirst=False, errors='coerce')
    # order_leave = pd.read_csv('/Users/jwt/PycharmProjects/dashboard/CPI/order_leave.csv')
    # order_leave['de_date_filed'] = order_leave['de_date_filed'].apply(pd.to_datetime, yearfirst=True, dayfirst=False,
    #                                                                   errors='coerce')
    # # df['Under Advisement Date'] = df.apply(lambda x: get_ua_date(x['Case ID'], x['DocNum'], ua), axis=1)
    # #
    # # df['Under Advisement Date'] = df['Under Advisement Date'].apply(pd.to_datetime, yearfirst=True,
    # #                                                                 dayfirst=False, errors='coerce')
    # #
    # # mask = df['Under Advisement Date'].notnull()
    # # df = df[mask]
    # # df.drop(['Date Filed.1'], inplace=True, axis=1)
    # #
    # # df['CMP To LTP Elapsed'] = df['Under Advisement Date'] - df['Date Filed']
    # #
    # # pp_ltp = pd.merge(df, order_leave, left_on='Case ID', right_on='de_caseid', how='left')
    # # pp_ltp = pp_ltp.drop(
    # #     ['de_caseid', 'de_date_enter', 'de_who_entered',
    # #      'initials', 'name', 'pr_type',
    # #      'pr_crttype'], axis=1)
    # # pp_ltp.rename(columns={'de_seqno': 'LTP DE SeqNum',
    # #                        'de_type': 'LTP Type',
    # #                        'dp_sub_type': 'LTP DP Type',
    # #                        'de_document_num': 'LTP DocNum',
    # #                        'dp_seqno': 'LTP DP SeqNum',
    # #                        'de_date_filed': 'LTP Date Filed'}, inplace=True)
    # #
    # # pp_ltp[['LTP DE SeqNum', 'LTP DP SeqNum', 'LTP DocNum']] = pp_ltp[
    # #     ['LTP DE SeqNum', 'LTP DP SeqNum', 'LTP DocNum']].fillna(0).astype(int)
    # #
    # # pp_ltp['LTP Date Filed'] = pp_ltp['LTP Date Filed'].apply(pd.to_datetime, yearfirst=True,
    # #                                                           dayfirst=False, errors='coerce')
    # #
    # # pp_ltp['LTP to Order Elapsed'] = pp_ltp['LTP Date Filed'] - pp_ltp['Under Advisement Date']
    # # pp_ltp.dropna(subset=['LTP Date Filed'], inplace=True)
    # # pp_ltp.dropna(subset=['LTP to Order Elapsed'], inplace=True)
    #


# def ua__dates(caseid):
#     print(caseid)
#     target = asyncio.run(get_docket_entries_for_case(caseid))
#     results = get_ua_date(caseid, target)
#     return results


if __name__ == '__main__':
    import time

    start_time = time.perf_counter()
    df = pd.read_csv('/Users/jwt/PycharmProjects/dashboard/CPI/prose_merged.csv')
    df[['Date Filed', 'Date Terminated', 'DateAgg']] = df[['Date Filed', 'Date Terminated', 'DateAgg']].apply(
        pd.to_datetime, yearfirst=True,
        dayfirst=False, errors='coerce')
    mask = df['Date Filed'] <= datetime.datetime(2018, 2, 1)
    df1 = df[mask]
    caseids = df1['Case ID'].tolist()
    # caseids = [41091, 41099, 41106, 41107, 41108, 41115, 41121, 41124, 41130, 41131, 41132, 41155, 41145, 41148, 41158]
    asyncio.run(main(*caseids))
    end_time = time.perf_counter()
    elapsed_time = end_time - start_time
    print(f"Elapsed run time: {elapsed_time} seconds")
