import pickle
import datetime

import pandas as pd
import numpy as np
from colorama import Fore, Style


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


def _find_complaint(case_dates, target):
    mask = target['de_type'] == 'cmp'
    cmp = target[mask]
    if not cmp.empty:
        cmp_docnum = cmp['de_document_num'].iloc[0].astype(int)
        cmp_docnum = f'[{int(cmp_docnum)}]'
        cmp_date = cmp['de_date_filed'].iloc[0]
        case_dates.append({'cmp_date': cmp_date, 'cmp_docnum': cmp_docnum})

        # was an amended complaint or 2255 motion filed?
        events = ['amdcmp', 'pamdcmp', '2255']
        mask = target['dp_sub_type'].isin(events)
        amdcmp = target[mask]
        if not amdcmp.empty:
            cmp_docnum = amdcmp['de_document_num'].iloc[0].astype(int)
            cmp_docnum = f'[{int(cmp_docnum)}]'
            amdcmp_date = amdcmp['de_date_filed'].iloc[0]
            case_dates.append({'amdcmp_date': amdcmp_date, 'amdcmp_docnum': cmp_docnum})
        else:
            amdcmp_date = np.nan
            amdcmp_docnum = np.nan
            case_dates.append({'amdcmp_date': amdcmp_date, 'amdcmp_docnum': amdcmp_docnum})
    else:
        cmp_docnum = np.nan
        cmp_date = np.nan
        case_dates.append({'cmp_date': cmp_date, 'cmp_docnum': cmp_docnum})

    return case_dates


def _get_screening_date(case_dates, target):
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


def _get_ifp_date(case_dates, target):
    mask = target['dp_sub_type'] == 'ifp'
    ifp = target[mask]
    if not ifp.empty:
        ifp_docnum = ifp['de_document_num'].iloc[0].astype(int)
        ifp_docnum = f'[{int(ifp_docnum)}]'
        ifp_date = ifp['de_date_filed'].iloc[0]
        case_dates.append({'ifp_date': ifp_date, 'ifp_docnum': ifp_docnum})
    else:
        ifp_docnum = '0'
        ifp_date = np.nan
        case_dates.append({'ifp_date': ifp_date, 'ifp_docnum': ifp_docnum})
    return case_dates


def _get_ua_date(case_dates, ua):
    if not ua.empty:
        ua_date = find_ua_date(case_dates[1]['cmp_docnum'], ua)
    # check for alternative ua dates
    if ua_date is np.nan and case_dates[3]['ifp_date'] is not np.nan:
        # use IFP motion date
        ua_date = find_ua_date(case_dates[3]['ifp_docnum'], ua)
    elif ua_date is np.nan and case_dates[4]['screening_date'] is not np.nan:
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


def _check_for_trust_fund_statement(case_dates, target):
    # Was the case dimissed due to lack or prisoner trust fund statement?
    mask = target['dp_sub_type'] == 'termpscs'
    term = target[mask]
    if not term.empty:
        term_date = term['de_date_filed'].iloc[0]
        case_dates.append({'tfs_date': term_date})
    else:
        case_dates.append({'tfs_date': np.nan})
    return case_dates


def _early_dismissal(case_dates, target):
    s1 = target.loc[target['dp_sub_type'] == 'voldism']
    s2 = target.loc[target['dp_sub_type'] == 'termcs']
    s3 = target.loc[target['dp_sub_type'] == 'prose2']
    dism = pd.concat([s1, s2, s3])
    # take the last date filed
    if not dism.empty:
        dis_date = dism['de_date_filed'].iloc[-1]
        case_dates.append({'dis_date': dis_date})
    else:
        case_dates.append({'dis_date': np.nan})


def get_ua_date(case_id: int, target: pd.DataFrame) -> pd.DataFrame:
    """
    """
    case_dates = []
    print(Fore.BLUE + f'Getting ua dates for {case_id}...', flush=True)
    case_dates.append({'case_id': case_id})
    st = 'forma pauperis'
    # locate complaints
    case_dates = _find_complaint(case_dates, target)
    if case_dates[1]['cmp_date']:
        # check for ifp motion
        case_dates = _get_ifp_date(case_dates, target)
        case_dates = _get_screening_date(case_dates, target)
        # Get all under advisement entries
        mask = target['dp_sub_type'] == 'madv'
        ua = target[mask]

    if not ua.empty:
        # Find the under advisement date for the complaint
        case_dates = _get_ua_date(case_dates, ua)
    else:
        case_dates = _check_for_trust_fund_statement(case_dates, target)

    if ua.empty:
        # Was there a voluntary dismissal or termination due to no prisoner trust fund statement?
        case_dates = _early_dismissal(case_dates, target)

    print(Fore.WHITE + f'Finished getting case dates for: {case_id}')
    return case_dates


def main():
    # get saved docket entries
    with open('dataframes2018-2022.pkl', 'rb') as f:
        dataframes = pickle.load(f)

    df = pd.concat(dataframes)
    # cleanup to assist with string matching and to eliminate unnecessary columns
    df = df.drop(['de_date_enter', 'de_who_entered', 'initials', 'name', 'pr_type', 'pr_crttype'],
                 axis=1)
    df['de_type'] = df['de_type'].str.strip()
    df['dp_type'] = df['dp_type'].str.strip()
    df['dp_sub_type'] = df['dp_sub_type'].str.strip()

    ua_dates = []
    cases = pd.read_csv('/Users/jwt/PycharmProjects/dashboard/CPI/prose_merged.csv')
    cases[['Date Filed', 'Date Terminated', 'DateAgg']] = cases[['Date Filed', 'Date Terminated', 'DateAgg']].apply(
        pd.to_datetime, yearfirst=True,
        dayfirst=False, errors='coerce')
    cases['Case ID'] = cases['Case ID'].astype(int)
    mask = cases['Date Filed'] <= datetime.datetime(2018, 5, 31)
    df1 = cases[mask]
    caseids = df1['Case ID'].tolist()
    # caseids = cases['Case ID'].tolist()
    # caseids = [41091, 41099, 41106]

    for caseid in caseids:
        target = df.loc[df['de_caseid'] == caseid]
        ua_dates.append(get_ua_date(caseid, target))

    with open('ua_dates.pkl', 'wb') as f:
        pickle.dump(ua_dates, f, pickle.HIGHEST_PROTOCOL)

    # with open('ua_dates.pkl', 'rb') as f:
    #     ua_dates = pickle.load(f)


if __name__ == '__main__':
    main()
