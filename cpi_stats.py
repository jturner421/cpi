import datetime
import pandas as pd
import numpy as np
from configuration.config import Config
import httpx
import datetime
import json

from db.dbsession import get_postgres_db_session, DbSession
from services.api_services import ApiSession
from services import civil_case_strategy, civil_case_service

get_postgres_db_session()
session = DbSession.factory()
config = Config()
api = ApiSession()
api_base_url = config.base_api_url
cases_url = config.civil_cases_endpoint


def get_data(case_ids, access_token, url, params, event, overall_type):
    headers = {'Authorization': f'Bearer {access_token}', 'Content-Type': 'application/json'}
    if event:
        data = {'case_ids': case_ids, 'subtype': event}
    if overall_type:
        data = {'case_ids': case_ids, 'overall_type': overall_type}
    r = httpx.post(url, params=params, headers=headers, json=data, verify=False, timeout=None)
    df = pd.DataFrame(r.json()['data'])
    return df

def get_docket_entries_for_case(caseid):
    url = f'{api_base_url}/cases/entries/{caseid}'
    headers = {'Authorization': f'Bearer {api.access_token}', 'Content-Type': 'application/json'}
    params = {'documents': False, 'docket_text': True}
    r = httpx.get(url, params=params, headers=headers, verify=False, timeout=None)
    df = pd.DataFrame(r.json()['data'])
    return df

def create_event(event_type: tuple) -> list:
    event = []
    stage_list = []
    for e in event_type:
        stage_list.append(e)
    event.append(stage_list)
    return event


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




def get_ua_date(case_id: int, doc_num, df: pd.DataFrame):
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

    target = df.loc[(df['de_caseid'] == case_id)]
    target = target.sort_values(by=['de_date_filed'], ascending=True)
    ua_date = target['de_date_filed'].iloc[0]

    # strings = ['Leave to Proceed',
    #            'In Forma Pauperis Screening',
    #            'Prison Litigation Reform Act',
    #            'Motion for Screening of Complaint',
    #            'PRLA Screening'
    #            'Paid Prisoner']
    # strings = ['Leave to Proceed']
    # assumes that first UA event is the one that we want
    try:
        # doc_num = f'[{int(doc_num)}]'
        if not target.empty:
            # return target['de_date_filed'].iloc[0]
            for index, row in target.iterrows():
                for string in strings:
            #         if string in row['dt_text']:
            #             return row['de_date_filed']
    except ValueError:
        print(row['dt_text'])
        pass


url = f'{api_base_url}{cases_url}'
start_date = datetime.date(2018, 1, 1)
end_date = datetime.date(2022, 5, 31)
payload = {"start_date": start_date, "end_date": end_date}

headers = {'Authorization': f'Bearer {api.headers}'}

stats = civil_case_strategy.CivilByDate().get_cases(start_date=start_date,
                                                    end_date=end_date)
# save main dataframe to csv
stats.df.to_csv('/Users/jwt/PycharmProjects/dashboard/CPI/civil_cases_2018-2022.csv', index=False)

# filter dataframe to return cases where IsProse is y
df = pd.read_csv('/Users/jwt/PycharmProjects/dashboard/CPI/civil_cases_2018-2022.csv')
mask = df['IsProse'] == 'y'
pro_se = df[mask]

# retrieve complaints from ecf for cases
url = f'{api_base_url}{config.docket_entries_by_case_and_type}'
headers = {'Authorization': f'Bearer {api.access_token}', 'Content-Type': 'application/json'}
params = {'documents': False, 'docket_text': False}
case_ids = pro_se['Case ID'].to_list()
event = create_event(('cmp', 'cmp'))
complaints = get_data(case_ids, api.access_token, url, params, event=event,
                      overall_type=None)
complaints.to_csv('/Users/jwt/PycharmProjects/dashboard/CPI/complaints.csv', index=False)


event = create_event(('motion', 'pwrithc'))
habeas_complaints = get_data(case_ids, api.access_token, url, params, event=event,
                             overall_type=None)
habeas_complaints.to_csv('/Users/jwt/PycharmProjects/dashboard/CPI/habeas_complaints.csv', index=False)

params = {'documents': False, 'docket_text': False}
event = create_event(('motion', '2255'))
motion_2255 = get_data(case_ids, api.access_token, url, params, event=event,
                       overall_type=None)
motion_2255.to_csv('/Users/jwt/PycharmProjects/dashboard/CPI/motion_2255.csv', index=False)

# notice of removal
params = {'documents': False, 'docket_text': False}
event = create_event(('notice', 'ntcrem'))
notice_removal = get_data(case_ids, api.access_token, url, params, event=event,
                          overall_type=None)
notice_removal.to_csv('/Users/jwt/PycharmProjects/dashboard/CPI/notice_removal.csv', index=False)

# emergency injunctions

params = {'documents': False, 'docket_text': False}
event = create_event(('motion', 'emerinj'))
injunctions = get_data(case_ids, api.access_token, url, params, event=event,
                          overall_type=None)
injunctions.to_csv('/Users/jwt/PycharmProjects/dashboard/CPI/injunctions.csv', index=False)
# dummy screening

params = {'documents': False, 'docket_text': False}
event = create_event(('motion', 'dummyscr'))
dummyscr = get_data(case_ids, api.access_token, url, params, event=event,
                          overall_type=None)
dummyscr.to_csv('/Users/jwt/PycharmProjects/dashboard/CPI/dummyscr.csv', index=False)

# bk appeals
params = {'documents': False, 'docket_text': False}
event = create_event(('appeal', 'bkntc'))
bk_appeal = get_data(case_ids, api.access_token, url, params, event=event,
                          overall_type=None)
bk_appeal.to_csv('/Users/jwt/PycharmProjects/dashboard/CPI/bk_appeal.csv', index=False)

params = {'documents': False, 'docket_text': False}
event = create_event(('motion', 'tro'))
tro = get_data(case_ids, api.access_token, url, params, event=event,
                          overall_type=None)
tro.to_csv('/Users/jwt/PycharmProjects/dashboard/CPI/tro.csv', index=False)

params = {'documents': False, 'docket_text': False}
event = create_event(('motion', 'setagr'))
agr = get_data(case_ids, api.access_token, url, params, event=event,
                          overall_type=None)
agr.to_csv('/Users/jwt/PycharmProjects/dashboard/CPI/tro.csv', index=False)

# merge complaint dataframes
complaints_df = create_merged_df(pro_se, complaints)

# habeas corpus complaints
habeas_df = create_merged_df(pro_se, habeas_complaints)

# 2255 motions
df_2255 = create_merged_df(pro_se, motion_2255)

# Notice of Removals
notice_df = create_merged_df(pro_se, notice_removal)

injunctions_df = create_merged_df(pro_se, injunctions)
dummyscr_df = create_merged_df(pro_se, dummyscr)
bk_appeal_df = create_merged_df(pro_se, bk_appeal)
tro_df = create_merged_df(pro_se, tro)
agr_df = create_merged_df(pro_se, agr)



df = pd.concat([complaints_df, habeas_df, df_2255, notice_df, injunctions_df, dummyscr_df, bk_appeal_df,tro_df,agr_df])
df.drop_duplicates(subset=['Case ID'], keep='first', inplace=True)
df.to_csv('/Users/jwt/PycharmProjects/dashboard/CPI/prose_complaints.csv', index=False)


url = "http://localhost:8000/cmecf/v1/entries/dktype/cases"
event = create_event(('utility', 'madv'))
params = {'documents': False, 'docket_text': True}
ua = get_data(case_ids, api.access_token, url, params, event=event, overall_type=None)
ua = ua.drop(['de_type', 'dp_type', 'dp_sub_type', 'de_date_enter', 'de_who_entered', 'initials', 'name', 'pr_crttype',
              'pr_type'], axis=1)
ua['de_date_filed'] = ua['de_date_filed'].apply(pd.to_datetime, yearfirst=True,
                                                dayfirst=False, errors='coerce')
ua.to_csv('/Users/jwt/PycharmProjects/dashboard/CPI/ua.csv', index=False)
#

case_ids = df['Case ID'].to_list()
event = create_event(('order', 'leave'))
params = {'documents': False, 'docket_text': False}
order_leave = get_data(case_ids, api.access_token, url, params, event=event,
                       overall_type=None)
order_leave.to_csv('/Users/jwt/PycharmProjects/dashboard/CPI/order_leave.csv', index=False)
# order_leave.to_csv('pro_se_leave_to_proceed_2018-2022.csv', index=False)

# isolate prisoner petitions
# cases = pd.read_csv('pro_se_terminated_civil_cases_2018-2022.csv')
# pp = pro_se_terminated.loc[pro_se_terminated['Group'] == 'Prisoner Petitions']
hab = pro_se.loc[pro_se['Group'] == 'Habeas Corpus']
caseids = hab['Case ID'].to_list()
url = "http://localhost:8000/cmecf/v1/entries/dktype/cases"
# url = f'{api_base_url}{config.docket_entries_by_case_and_type}'
headers = {'Authorization': f'Bearer {api.access_token}', 'Content-Type': 'application/json'}
params = {'documents': False, 'docket_text': False}
event = create_event(('motion', '2255'))
hab_motions = get_data(caseids, api.access_token, url, params, event=event, overall_type=None)

# complaints = pd.read_csv('pro_se_complaints_2018-2022.csv')
prose_complaints = pd.merge(pro_se, complaints, left_on='Case ID', right_on='de_caseid', how='left')
prose_complaints = prose_complaints.drop(
    ['Cause of Action', 'Diversity Defendant', 'Diversity Plaintiff', 'IsProse', 'de_caseid',
     'de_date_enter', 'de_who_entered', 'initials', 'name', 'pr_type', 'pr_type', 'pr_crttype'], axis=1)
prose_complaints.drop_duplicates(subset=['Case ID'], keep='first', inplace=True)
prose_complaints['de_document_num'] = prose_complaints['de_document_num'].fillna(0).astype(int)
prose_complaints['de_seqno'] = prose_complaints['de_seqno'].fillna(0).astype(int)
prose_complaints['dp_seqno'] = prose_complaints['dp_seqno'].fillna(0).astype(int)
prose_complaints = prose_complaints.rename(columns={'de_seqno': 'CMP DE SeqNum',
                                                    'dp_seqno': 'CMP DP SeqNum',
                                                    'de_document_num': 'CMP DocNum',
                                                    'dp_type': 'CMP DP Type',
                                                    'dp_sub_type': 'CMP DP SubType',
                                                    'de_date_filed': 'CMP Date Filed'})
prose_complaints['CMP Date Filed'] = prose_complaints['CMP Date Filed'].apply(pd.to_datetime, yearfirst=True,
                                                                              dayfirst=False, errors='coerce')

                                                          dayfirst=False, errors='coerce')

df['Under Advisement Date'] = df.apply(lambda x: get_ua_date(x['Case ID'], ua),axis=1)

df['Under Advisement Date'] = df['Under Advisement Date'].apply(pd.to_datetime, yearfirst=True,
                                                                        dayfirst=False, errors='coerce')

pp_ifp['CMP To LTP Elapsed'] = pp_ifp['Under Advisement Date'] - pp_ifp['Date Filed']

pp_ltp = pd.merge(pp_ifp, order_leave, left_on='Case ID', right_on='de_caseid', how='left')


pp_ltp.rename(columns={'de_seqno': 'LTP DE SeqNum',
                       'de_type': 'LTP Type',
                       'dp_sub_type': 'LTP DP Type',
                       'de_document_num': 'LTP DocNum',
                       'dp_seqno': 'LTP DP SeqNum',
                       'de_date_filed': 'LTP Date Filed'}, inplace=True)



# mask = pp_ifp['Under Advisement Date'].isnull()
# pp_ifp_null = pp_ifp[mask]
# pp_ifp_term = pp_ifp[~mask]
# pp_ifp_term['CMP To LTP Elapsed'] = pp_ifp_term['Under Advisement Date'] - pp_ifp_term['Date Filed']
# pp_ifp_term['CMP To LTP Elapsed'] = pp_ifp_term['CMP To LTP Elapsed'].dt.days.astype(int)
#
# pp_ifp_term.to_csv('/Users/jwt/PycharmProjects/dashboard/utilities/pro_se_pp_ifp_2018-2022.csv', index=False)
#
# df = pd.read_csv('/Users/jwt/PycharmProjects/dashboard/utilities/pro_se_pp_ifp_2018-2022.csv')
# case_ids = pp_ifp['Case ID'].to_list()
#
# pp_ltp = pp_ltp.drop(['de_caseid', 'de_seqno', 'de_type',
#                       'de_document_num', 'dp_type', 'dp_sub_type', 'dp_seqno', 'de_who_entered',
#                       'initials', 'name', 'pr_type', 'pr_crttype'], axis=1)
#
# pp_ltp[['Date Filed', 'Date Terminated']] = pp_ltp[
#     ['Date Filed', 'Date Terminated']].apply(pd.to_datetime, yearfirst=True, dayfirst=False, errors='coerce')
#
# mask = pp_ltp['de_date_filed'].isnull()
# pp_ltp_null = pp_ltp[mask]
# pp_ltp_term = pp_ltp[~mask].copy(deep=True)
# pp_ltp_term['Under Advisement Date'] = pp_ltp_term['Under Advisement Date'].apply(pd.to_datetime, yearfirst=True,
#                                                                                   dayfirst=False, errors='coerce')
# pp_ltp_term[['de_date_filed', 'de_date_enter']] = pp_ltp_term[['de_date_filed', 'de_date_enter']].apply(pd.to_datetime,
#                                                                                                         yearfirst=True,
#                                                                                                         dayfirst=False,
#                                                                                                         errors='coerce')
# pp_ltp_term['LTP to Order Elapsed'] = pp_ltp_term['de_date_filed'] - pp_ltp_term['Under Advisement Date']
# pp_ltp_term['LTP to Order Elapsed'] = pp_ltp_term['LTP to Order Elapsed'].dt.days.astype(int)
# pp_ltp_term.to_csv('/Users/jwt/PycharmProjects/dashboard/utilities/pro_se_pp_ltp_2018-2022.csv', index=False)
