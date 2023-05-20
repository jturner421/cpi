from collections import OrderedDict
from datetime import datetime
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional

from flashtext import KeywordProcessor

import dill
import pandas as pd
from colorama import Fore

from util import timeit
from services.dataframe_services import create_dataframe_docket_entries

date_format = '%Y-%m-%d'


class DismissalType(Enum):
    voldism = 'Voluntary Dismissal'
    termpscs = 'No Trust Fund Statement'
    termcs = 'Civil Case Terminated'
    dismcmp = 'Complaint Dismissed'


@dataclass
class CaseDates:
    caseid: int
    case_type: Optional[str] = None
    case_number: Optional[str] = None
    judge = Optional[str]
    nature_of_suit: Optional[str] = None
    case_group: Optional[str] = None
    transfer_date: Optional[datetime] = None
    complaint_docnum: str = "0"
    complaint_date: Optional[datetime] = None
    complaint_dismissal_date: Optional[datetime] = None
    ifp_submission_date: Optional[datetime] = None
    ifp_docnum: Optional[int] = None
    order_to_submit_trust_fund_date: Optional[datetime] = None
    trust_fund_received_date: Optional[datetime] = None
    trust_fund_docnum: Optional[int] = None
    ifp_order_granted_date: Optional[datetime] = None
    ifp_order_denied_date: Optional[datetime] = None
    partial_payment_date: Optional[datetime] = None
    full_fee_paid: Optional[bool] = False
    screening_docnum: str = "0"
    screening_date: Optional[datetime] = None
    screening_ua_date: Optional[datetime] = None
    ua_date: Optional[datetime] = None
    warden_letter_date: Optional[datetime] = None
    dismissal_date_for_no_trust_fund_statement: Optional[datetime] = None
    terminated_date: Optional[datetime] = None
    never_ua: Optional[bool] = False
    ua_dates: list = field(default_factory=list)
    prose_orders: list = field(default_factory=list)
    trust_fund_dates: list = field(default_factory=list)

    def dict(self):
        self._calculate_trust_fund_order_request_date()
        self._calculate_ifp_order_granted_date()
        self._calculate_ifp_order_denied_date()
        self._calculate_ua_date()

        if self.full_fee_paid:
            try:
                if self.trust_fund_received_date > self.ua_date:
                    self.trust_fund_received_date = pd.NaT
            except TypeError:
                pass
            try:
                if self.ifp_submission_date > self.ua_date:
                    self.ifp_submission_date = pd.NaT
            except TypeError:
                pass

        _dict = self.__dict__.copy()
        if type(_dict['complaint_date']) is str:
            _dict['complaint_date'] = datetime.strptime(_dict['complaint_date'], date_format)
        if type(_dict['ifp_submission_date']) is str:
            _dict['ifp_submission_date'] = datetime.strptime(_dict['ifp_submission_date'], date_format)
        if type(_dict['screening_date']) is str:
            _dict['screening_date'] = datetime.strptime(_dict['screening_date'], date_format)
        # only applies to transferred cases

        del _dict['ua_dates']
        del _dict['prose_orders']
        del _dict['complaint_docnum']
        del _dict['screening_docnum']
        del _dict['trust_fund_docnum']
        del _dict['trust_fund_dates']
        del _dict['complaint_dismissal_date']
        del _dict['screening_ua_date']
        del _dict['ifp_docnum']
        return _dict

    def _calculate_ifp_order_granted_date(self):
        keyword_processor = KeywordProcessor()
        keyword_dict = {'OrderIFP': ["order"],
                        'IFP': ["ifp request"],
                        'PFF': ["partial", "filling fee"],
                        'Assessed': ["assessed"]}
        keyword_processor.add_keywords_from_dict(keyword_dict)
        for order in self.prose_orders:
            results = keyword_processor.extract_keywords(order['dkt_text'])
            if len(results) > 0:
                if all([x in results for x in ['OrderIFP', 'IFP', 'PFF', 'Assessed']]):
                    self.ifp_order_granted_date = datetime.strptime(order['ua_date'], date_format).date()

    def _calculate_ifp_order_denied_date(self):
        keyword_processor = KeywordProcessor()
        keyword_dict = {'OrderIFP': ["order"],
                        'IFP': ["ifp"],
                        'DENIED': ["denied", "ineligible"]}
        keyword_processor.add_keywords_from_dict(keyword_dict)
        for order in self.prose_orders:
            results = keyword_processor.extract_keywords(order['dkt_text'])
            if len(results) > 0:
                if all([x in results for x in ['OrderIFP', 'IFP', 'DENIED']]):
                    self.ifp_order_denied_date = datetime.strptime(order['ua_date'], date_format).date()

    def _calculate_trust_fund_order_request_date(self):
        keyword_processor = KeywordProcessor()
        keyword_dict = {'OrderTF': ["order", "submit"],
                        'TF': ["trust fund account statement"],
                        'IFP': ["ifp"],
                        'FF': ["plaintiff", "filling fee"],
                        'INELIGIBLE': ["ineligible"],
                        'FIVE': ["$5", "5", "5.00"]}
        keyword_processor.add_keywords_from_dict(keyword_dict)
        for order in self.prose_orders:
            results = keyword_processor.extract_keywords(order['dkt_text'])
            if len(results) > 0:
                if all([x in results for x in ['OrderTF', 'TF', 'FF']]) or \
                        all([x in results for x in ['OrderTF', 'IFP', 'INELIGIBLE']]) or \
                        all([x in results for x in ['OrderTF', 'TF', 'FIVE']]):
                    self.order_to_submit_trust_fund_date = datetime.strptime(order['ua_date'], date_format).date()

    def _calculate_ua_date(self):
        # credit: https://stackoverflow.com/questions/9427163/remove-duplicate-dict-in-list-in-python (author:fourtheye)
        self.ua_dates = list(OrderedDict((frozenset(item.items()), item) for item in self.ua_dates).values())

        # ua_dates = pd.DataFrame(self.ua_dates)
        if len(self.ua_dates) > 0:
            keyword_processor = KeywordProcessor()
            keyword_dict = {'UA': ["taken under advisement"],
                            'LTP': ["leave to proceed"],
                            'MIFP': ["forma", "pauperis"],
                            'PP': ["paid", "prisoner"],
                            'SCREENING': ["screening"],
                            'PETITION': ["petition"],
                            'WRIT': ["writ"],
                            'HABEAS': ["habeas", "corpus"]}
            keyword_processor.add_keywords_from_dict(keyword_dict)
            ua_dates = []
            for order in self.ua_dates:
                results = keyword_processor.extract_keywords(order['ua_text'])
                if len(results) > 0:
                    if all([x in results for x in ['UA', 'LTP', 'MIFP']]) \
                            or all([x in results for x in ['UA', 'PP', 'SCREENING']]) \
                            or all([x in results for x in ['UA', 'PETITION', 'WRIT', 'HABEAS']]):
                        ua_dates.append(datetime.strptime(order['ua_date'], date_format).date())
            if len(ua_dates) > 0:
                self.ua_date = min(ua_dates)
        else:
            self.ua_date = None
            self.never_ua = True

    def _process_text(self):
        """
        Function to process text to find relevant dates and orders
        """


def _find_target_dates(df: pd.DataFrame, keywords: List[str], check_all: bool = False) -> List[tuple]:
    """
    Function to find dates for a case.  Utilizes keywords to find candidate dates and appends
    to a list.

    :param df: dataframe of events for that case
    :param keywords: list of keywords to search for
    """
    df['dt_text'] = df['dt_text'].str.lower()
    target_dates = []

    for index, row in df.iterrows():
        a_string = row['dt_text']
        if check_all:
            if all(x in a_string for x in keywords):
                target_dates.append((row['de_date_filed'], row['dt_text'], row['de_seqno']))
        else:
            if any(x in a_string for x in keywords):
                target_dates.append((row['de_date_filed'], row['dt_text'], row['de_seqno']))
    return target_dates


def _find_complaint(target: pd.DataFrame) -> CaseDates:
    """
    Function to find the complaint date for a case.  Utilizes keywords to find initiating documents based on events. If
    multiple events found, the first date is used as the complaint filing date.  Also looks to see if any amended
    complaints were filed

    """
    case = CaseDates(caseid=target['de_caseid'].iloc[0])
    cmp = target.query('dp_type == "motion" and dp_sub_type == "2255" or dp_sub_type=="cmp" or dp_sub_type=="pwrithc" '
                       'or dp_sub_type == "ntcrem" or dp_sub_type == "emerinj" or dp_sub_type == "bkntc" '
                       'or dp_sub_type == "setagr" ')
    if not cmp.empty:
        cmp.sort_values(by=['de_seqno'], inplace=True)
        case.complaint_docnum = cmp['de_document_num'].iloc[0].astype(int)
        case.complaint_docnum = f'[{int(case.complaint_docnum)}]'
        case.complaint_date = datetime.strptime(cmp['de_date_filed'].min(), date_format).date()
    return case


def _get_transfer_date(case: CaseDates, target: pd.DataFrame) -> CaseDates:
    """
    Function to find the transfer date for a case.

    """
    trf = target.query('dp_sub_type == "distin"')
    trf.drop_duplicates(subset='de_seqno', keep='first', inplace=True)
    if not trf.empty:
        case.transfer_date = trf['de_date_filed'].iloc[0]
    return case


def _get_screening_date(case: CaseDates, target: pd.DataFrame) -> CaseDates:
    """
    Function to find the screening date for a case where there was no ifp request. 
    
    :param case: CaseDates object
    :param target: dataframe of events for a case
    :return: CaseDates object
    -------

    """
    # Check for dummy screening motion
    screening = target.query('dp_sub_type == "dummyscr" or dp_sub_type == "pdpro" ')
    if not screening.empty:
        case.screening_docnum = screening['dp_seqno'].iloc[0].astype(int)
        case.screening_docnum = f'[{int(case.screening_docnum)}]'
        case.screening_date = screening['de_date_filed'].iloc[0]
    else:
        case.screening_docnum = '0'
    return case


def get_prisoner_trust_fund_statement_date(case_dates: CaseDates, target: pd.DataFrame) -> pd.DataFrame:
    trust_fund_statement = target.loc[target['dp_sub_type'] == 'trfund']
    if not trust_fund_statement.empty:
        trust_fund_docnum = trust_fund_statement['de_document_num'].iloc[0].astype(int)
        trust_fund_docnum = f'[{int(trust_fund_docnum)}]'
        trust_fund_date = trust_fund_statement['de_date_filed'].iloc[0]
        case_dates.trust_fund_docnum = trust_fund_docnum
        case_dates.trust_fund_received_date = datetime.strptime(trust_fund_date, date_format).date()
    return case_dates


def _get_ifp_date(case_dates: CaseDates, target: pd.DataFrame) -> pd.DataFrame:
    """
    Function to find the ifp date for a case if it exists.

    :param case_dates: CaseDates object
    :param target: dataframe of events for a case
    :return: CaseDates object
    -------

    """
    ifp = target.loc[target['dp_sub_type'] == 'ifp']
    if not ifp.empty:
        ifp_docnum = ifp['de_document_num'].iloc[0].astype(int)
        ifp_docnum = f'[{int(ifp_docnum)}]'
        ifp_date = ifp['de_date_filed'].iloc[0]
        case_dates.ifp_docnum = ifp_docnum
        case_dates.ifp_submission_date = datetime.strptime(ifp_date, date_format).date()
    else:
        case_dates.ifp_docnum = '0'
    return case_dates


def _get_ua_dates_for_later_processing(case_dates, target):
    if case_dates.full_fee_paid:
        ua = target.loc[target['dp_sub_type'] == 'pdpro']
        if not ua.empty:
            for index, row in ua.iterrows():
                case_dates.ua_dates.append({'ua_date': row['de_date_filed'], 'ua_text': row['dt_text']})
    else:
        ua = target.loc[(target['dp_sub_type'] == 'madv') | (target['dp_sub_type'] == 'rel')
                        | (target['dp_sub_type'] == 'termddl')]
        if not ua.empty:
            matches = ["screening", 'complaint', 'pauperis', 'habeas', 'reopen',
                       'social security', 'bankruptcy', 'prepayment', '2255']
            ua_dates = _find_target_dates(ua, matches)
            if ua_dates:
                for ua_date in ua_dates:
                    case_dates.ua_dates.append({'ua_date': ua_date[0], 'ua_text': ua_date[1]})
    return case_dates


def combine_docket_text_into_one_row(target: pd.DataFrame) -> pd.DataFrame:
    replacement_text = target.groupby('de_seqno')['dt_text'].apply(' '.join).reset_index()
    target.drop(['dt_text'], axis=1, inplace=True)
    target.drop_duplicates(subset=['de_seqno'], keep='first', inplace=True)
    target = target.merge(replacement_text, left_on='de_seqno', right_on='de_seqno', how='left')
    return target


def _check_for_dismissal_due_to_no_trust_fund_statement(case_dates: CaseDates, target: pd.DataFrame) -> CaseDates:
    # Was the case dismissed due to lack of prisoner trust fund statement?
    mask = target['dp_sub_type'] == 'termpscs'
    term = target[mask]
    if not term.empty:
        term_date = term['de_date_filed'].min()
        case_dates.dismissal_date_for_no_trust_fund_statement = datetime.strptime(term_date, date_format).date()
    return case_dates


def _early_dismissal(case_dates, target) -> CaseDates:
    """
    Function to check for voluntary dismissal or other dismissal prior to screening 
   
    :param case_dates: CaseDates object
    :param target: dataframe of events for a case

    """
    dism = target.loc[target['dp_sub_type'] == 'voldism']
    dism.drop_duplicates(subset='de_seqno', keep='first', inplace=True)
    # take the last date filed
    if not dism.empty:
        dism.sort_values(by=['de_date_filed'], inplace=True, ascending=True)
        for row in dism.iterrows():
            dis_date = dism['de_date_filed'].iloc[0]
            dis_reason = dism['dp_sub_type'].iloc[0]
            dis_text = dism['dt_text'].iloc[0]
            type = dism['dp_sub_type'].iloc[0]
            case_dates.early_dismissal_dates.append({'dis_date': dis_date, 'dis_reason': dis_reason,
                                                     'dis_text': dis_text, 'type': type})
    return case_dates


def _get_partial_fee_paid_date(case_dates, target):
    fee = target.loc[target['dp_sub_type'] == 'fee']
    if not fee.empty:
        fee.sort_values(by=['de_date_filed'], inplace=True, ascending=True)
        case_dates.partial_payment_date = datetime.strptime(fee['de_date_filed'].iloc[0], date_format).date()
        if '400' in fee['dt_text'].iloc[0]:
            case_dates.full_fee_paid = True

    return case_dates


def _get_wadren_letter_date(case_dates, target):
    warden = target.loc[target['dp_sub_type'] == 'wardltr']
    if not warden.empty:
        case_dates.warden_letter_date = datetime.strptime(warden['de_date_filed'].iloc[0], date_format).date()
    return case_dates


def get_case_milestone_dates(case_id: int, target: pd.DataFrame, case_type: str, case_number: str, judge: str,
                             terminated_date: datetime, nature_of_suit: str, case_group: str) -> pd.DataFrame:
    """
    Wrapper Function to get case milestone dates for a case. Milestones include:
    Complaint, Screening, Under Advisement, IFP Motion, Dismissal, Reopen, Leave to Proceed, Pretrial Conference,
    """
    case_dates = []
    print(Fore.BLUE + f'Getting ua dates for {case_id}...', flush=True)
    try:
        case_dates.append({'case_id': case_id, 'case_number': case_number})
        # locate complaints
        case_dates = _find_complaint(target)
        case_dates.case_type = case_type
        case_dates.case_number = case_number
        case_dates.judge = judge
        case_dates.terminated_date = terminated_date
        case_dates.nature_of_suit = nature_of_suit
        case_dates.case_group = case_group
        case_dates = _get_transfer_date(case_dates, target)
        case_dates = _get_ifp_date(case_dates, target)
        case_dates = _get_screening_date(case_dates, target)
        case_dates = get_prisoner_trust_fund_statement_date(case_dates, target)
        case_dates = _get_partial_fee_paid_date(case_dates, target)
        case_dates = _check_for_dismissal_due_to_no_trust_fund_statement(case_dates, target)
        case_dates = _get_ua_dates_for_later_processing(case_dates, target)
        case_dates = _get_wadren_letter_date(case_dates, target)
        if case_dates.complaint_date or case_dates.screening_date:
            # check for under advisement
            case_dates = _check_for_prose_orders(case_dates, target)
        print(Fore.WHITE + f'Finished getting case dates for: {case_id}')
        case_dates_dict = case_dates.dict()
        return case_dates_dict
    except IndexError as e:
        print(Fore.RED + f'No dates found for {case_id}: {e}', flush=True)


def _check_for_prose_orders(case_dates, target):
    orders = target.loc[((target['dp_type'] == 'order') & (target['dp_sub_type'] == 'prose2'))]
    if not orders.empty:
        orders.drop_duplicates(subset=['dp_seqno'], inplace=True)
        orders.sort_values(by=['de_date_filed'], inplace=True, ascending=True)
        for index, row in orders.iterrows():
            case_dates.prose_orders.append({'ua_date': row['de_date_filed'], 'dkt_text': row['dt_text']})
    return case_dates


def get_docker_entries_for_case(caseid, df_entries):
    target = df_entries.loc[df_entries['de_caseid'] == caseid]
    target['de_document_num'].fillna(0, inplace=True)
    target['de_document_num'] = target['de_document_num'].astype(int)
    target['dp_dpseqno_ptr'].fillna(0, inplace=True)
    target['dp_dpseqno_ptr'] = target['dp_dpseqno_ptr'].astype(int)
    target['dt_text'] = target['dt_text'].str.lower()
    target = combine_docket_text_into_one_row(target)
    return target


@timeit
def main():
    # get saved docket entries
    with open('data_files/green_belt_docket_entries.pkl', 'rb') as f:
        dataframes_entries = dill.load(f)

    df_entries = create_dataframe_docket_entries(dataframes_entries)
    df_entries['dt_text'] = df_entries['dt_text'].str.lower()

    # delete dataframes to free up memory
    del dataframes_entries
    ua_dates = []
    cases = pd.read_csv('data_files/civil_cases_2020-2023.csv')
    # filter dataframe to return cases where IsProse is y
    mask = cases['IsProse'] == 'y'
    cases = cases[mask]
    # cases = cases.loc[cases['Group'] != 'Habeas Corpus']
    # cases = cases.loc[cases['Group'] != 'Bankruptcy']
    cases[['Date Filed', 'Date Terminated', 'DateAgg']] = cases[['Date Filed', 'Date Terminated', 'DateAgg']].apply(
        pd.to_datetime, yearfirst=True,
        dayfirst=False, errors='coerce')
    cases = cases.drop(columns=['Diversity Plaintiff', 'Diversity Defendant', 'IsProse'])
    cases = cases.loc[cases['Date Filed'] >= datetime(2020, 1, 1)]
    cases['Case ID'] = cases['Case ID'].astype(int)
    cases.drop_duplicates(keep='first', inplace=True)
    caseids = cases['Case ID'].tolist()

    # caseids = []

    # gather information for each case and find the ua dates and deadlines
    for caseid in caseids:
        case_type = cases.loc[cases['Case ID'] == caseid, 'Group'].iloc[0]
        case_number = str.strip(cases.loc[cases['Case ID'] == caseid, 'Case Number'].iloc[0])
        case_judge = str.strip(cases.loc[cases['Case ID'] == caseid, 'Judge'].iloc[0])
        termination_date = cases.loc[cases['Case ID'] == caseid, 'Date Terminated'].iloc[0]
        nature_of_suit = cases.loc[cases['Case ID'] == caseid, 'NOS'].iloc[0]
        case_group = cases.loc[cases['Case ID'] == caseid, 'Group'].iloc[0]
        target = get_docker_entries_for_case(caseid, df_entries)
        if nature_of_suit == 510:
            print(Fore.RED + f'Case {caseid} is a motion to vacate case and will not be processed', flush=True)
        else:
            ua_dates.append(
                get_case_milestone_dates(caseid, target, case_type, case_number, case_judge, termination_date,
                                         nature_of_suit, case_group))
    # save objects to disk for later use
    with open('data_files/green_belt_dates.pkl', 'wb') as f:
        dill.dump(ua_dates, f)

    # df = calculate_intervals(df)


if __name__ == '__main__':
    main()
