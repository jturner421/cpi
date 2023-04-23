from collections import OrderedDict
from datetime import datetime
from dataclasses import dataclass, field
from enum import Enum
from typing import List
import re

import dill
import pandas as pd
from colorama import Fore

from util import timeit
from services.dataframe_services import create_merged_ua_dates_or_deadlines, cleanup_merged_deadlines, \
    create_dataframe_docket_entries, create_dataframe_deadlines, create_dataframe_hearings, calculate_intervals


class DismissalType(Enum):
    voldism = 'Voluntary Dismissal'
    termpscs = 'No Trust Fund Statement'
    termcs = 'Civil Case Terminated'
    dismcmp = 'Complaint Dismissed'


@dataclass
class CaseDeadlines:
    caseid: int
    pptcnf_date: datetime = None
    dispositve_deadline: datetime = None
    limine_deadline: datetime = None
    fptcnf_date: datetime = None
    trial_date: datetime = None

    def dict(self):
        if self.dispositve_deadline:
            _dict = self.__dict__.copy()
            return _dict
        else:
            pass


@dataclass
class CaseDates:
    caseid: int
    case_type: str = None
    complaint_docnum: str = "0"
    complaint_date: datetime = None
    complaint_dismissal_date: datetime = None
    amended_complaint_date: datetime = None
    amended_complaint_count: int = None
    case_reopen_count: int = None
    ifp_docnum: int = None
    ifp_date: datetime = None
    screening_docnum: str = "0"
    screening_date: datetime = None
    ua_date: datetime = None
    ltp_date: datetime = None
    transfer_date: datetime = None
    notice_of_appeal_date: datetime = None

    judgment_date_closing_case_prior_to_reopen: datetime = None
    initial_pretrial_conference_date: datetime = None
    dismissal_date_prior_to_screening: datetime = None
    dismissal_reason_prior_to_screening: str = None
    judgment_without_prejudice_date: datetime = None
    order_dismissing_complaints: list = field(default_factory=list)
    motion_for_reconsideration: list = field(default_factory=list)
    amended_complaints: list = field(default_factory=list)
    order_to_leave_dates: list = field(default_factory=list)
    order_dates: list = field(default_factory=list)
    case_reopen_dates: list = field(default_factory=list)
    ua_dates: list = field(default_factory=list)
    trust_fund_dates: list = field(default_factory=list)
    early_dismissal_dates: list = field(default_factory=list)
    judgment_dates: list = field(default_factory=list)
    dismissal_dates_prior_to_screening: list = field(default_factory=list)

    def dict(self):
        # start tolling time from date transferred to this court
        if self.transfer_date:
            self.complaint_date = datetime.strptime(self.transfer_date, '%Y-%m-%d')
        self._calculate_dismissal_date()
        if len(self.early_dismissal_dates):
            # Notice of Dismissal prior to screening
            self._calculate_dismissal_date_prior_to_screening()
        if len(self.case_reopen_dates) >= 1:
            case_reopen_dates = [datetime.strptime(date, '%Y-%m-%d') for date in self.case_reopen_dates]
            case_reopen_dates.sort()
            case_reopen_dates = set(self.case_reopen_dates)
            self.case_reopen_dates = list(case_reopen_dates)
        self._calculate_amended_complaint_date()
        if len(self.ua_dates) >= 1:
            self._calculate_ua_date()
        else:
            self.ua_date = None
            print(f' No UA date in {self.caseid}')

        self._calculate_ltp_date()

        if self.initial_pretrial_conference_date:
            self.initial_pretrial_conference_date = datetime.strptime(self.initial_pretrial_conference_date,
                                                                      '%Y-%m-%d')
        self.amended_complaint_count = len(self.amended_complaints)
        self.case_reopen_count = len(self.case_reopen_dates)
        _dict = self.__dict__.copy()
        if type(_dict['complaint_date']) is str:
            _dict['complaint_date'] = datetime.strptime(_dict['complaint_date'], '%Y-%m-%d')
        if type(_dict['ifp_date']) is str:
            _dict['ifp_date'] = datetime.strptime(_dict['ifp_date'], '%Y-%m-%d')
        if type(_dict['screening_date']) is str:
            _dict['screening_date'] = datetime.strptime(_dict['screening_date'], '%Y-%m-%d')
        del _dict['amended_complaints']
        del _dict['order_to_leave_dates']
        del _dict['ua_dates']
        del _dict['trust_fund_dates']
        del _dict['early_dismissal_dates']
        del _dict['complaint_docnum']
        del _dict['screening_docnum']
        del _dict['ifp_docnum']
        del _dict['case_reopen_dates']
        return _dict

    def _calculate_ltp_date(self):
        if len(self.order_to_leave_dates) >= 1:
            try:
                for i in range(len(self.order_to_leave_dates)):
                    if datetime.strptime(self.order_to_leave_dates[i]['ltp_date'], '%Y-%m-%d') >= self.ua_date:
                        self.ltp_date = datetime.strptime(self.order_to_leave_dates[i]['ltp_date'], '%Y-%m-%d')
                        continue
            except TypeError:
                self.ltp_date = datetime.strptime(self.order_to_leave_dates[-1]['ltp_date'], '%Y-%m-%d')

        else:
            self.ltp_date = None
            print(f' No Leave order in {self.caseid}')

    def _calculate_dismissal_date_prior_to_screening(self):
        # there may be more than one order so we use a list comprehension to get all orders that dismisses the complaint
        result = [element for element in self.early_dismissal_dates if element["dis_reason"] == "dismcmp"]
        self.dismissal_date_prior_to_screening = datetime.strptime(result[-1]['dis_date'], '%Y-%m-%d')
        self.dismissal_reason_prior_to_screening = DismissalType[result[-1]['dis_reason']].value

    def _calculate_dismissal_date(self):
        # get the date
        # [item for item in self.early_dismissal_dates]
        if len(self.order_dismissing_complaints) > 0:
            self.complaint_dismissal_date = datetime.strptime(self.order_dismissing_complaints[0]['dis_date'],
                                                              '%Y-%m-%d')

    def _calculate_amended_complaint_date(self):
        # Only use amended complaints prior to the leave to proceed date
        if self.ltp_date:
            amended_complaint_dates = [x for x in self.amended_complaints if
                                       datetime.strptime(x['amdcmp_date'], '%Y-%m-%d') < self.ltp_date]
        else:
            amended_complaint_dates = self.amended_complaints
        if len(amended_complaint_dates) > 0:
            amended_complaint_dates = pd.DataFrame(amended_complaint_dates)
            amended_complaint_dates['amdcmp_date'] = [datetime.strptime(x, '%Y-%m-%d') for x
                                                      in amended_complaint_dates['amdcmp_date']]
            orders = pd.DataFrame(self.order_dates)
            ltp_orders = pd.DataFrame(self.order_to_leave_dates)
            ltp_orders.sort_values(by='ltp_date', inplace=True)
            ltp_orders['ltp_date'] = [datetime.strptime(x, '%Y-%m-%d') for x in ltp_orders['ltp_date']]
            for index, row in ltp_orders.iterrows():
                amended_complaint_dates = amended_complaint_dates.loc[amended_complaint_dates['amdcmp_date'] <
                                                                      row['ltp_date']]
            # use the
            pass
            # amended_complaint_docnum = self.amended_complaints[-1]['amdcmp_docnum']

    def _calculate_ua_date(self):
        # credit: https://stackoverflow.com/questions/9427163/remove-duplicate-dict-in-list-in-python (author:fourtheye)
        self.ua_dates = list(OrderedDict((frozenset(item.items()), item) for item in self.ua_dates).values())
        # remove item from list of dicts if text matches "dismissed
        self.ua_dates = [x for x in self.ua_dates if "appeal" not in x['ua_text']]
        # check for judgment without prejudice
        if self.judgment_dates:
            jgm_dates = ([datetime.strptime(x['judgment_date'], '%Y-%m-%d') for x in self.judgment_dates if
                          "without prejudice" in x['judgment_text']])
            jgm_dates.sort()
            if len(jgm_dates) > 0:
                # set it to the last judgment without prejudice date recorded
                self.judgment_without_prejudice_date = jgm_dates[-1]

        # is there a reopen date?
        if len(self.case_reopen_dates) >= 1:
            reopen_date = datetime.strptime(self.case_reopen_dates[-1], '%Y-%m-%d')

        # set dismissal date prior to screening or judgment without prejudice date, whichever is later
        try:
            dismissal_date = max(self.dismissal_date_prior_to_screening, self.judgment_without_prejudice_date)
        except TypeError:
            pass
        # are there amended complaints?
        if self.amended_complaint_date:
            # if so extract the docnum
            amended_complaint_docnum = self.amended_complaints[-1]['amdcmp_docnum']
        else:
            amended_complaint_docnum = None

            # # Use onlu UA dates that are associated with the amended complaint(s)
            # ua_dates = [x for x in self.ua_dates if amended_complaint_docnum in x['ua_text']]
            # # Amended complaint could be filed but never go UA. This will result in an empty list. In this case we
            # # compile all the UA dates.
            # if len(ua_dates) == 0:
            #     ua_dates = [x for x in self.ua_dates]
        # else:
        #     # If no amended complaints, use all UA dates
        #     ua_dates = [x for x in self.ua_dates]
        if amended_complaint_docnum is None:
            matches = ['leave to proceed', 'pauperis', 'screening']
        else:
            matches = ['leave to proceed', amended_complaint_docnum, 'pauperis', "screening"]
        if len(self.ua_dates) > 0:
            for ua_date in self.ua_dates:
                if any(x in ua_date['ua_text'].lower() for x in matches):
                    try:
                        if datetime.strptime(ua_date['ua_date'], '%Y-%m-%d') > dismissal_date:
                            self.ua_date = datetime.strptime(ua_date['ua_date'], '%Y-%m-%d')
                            continue  # self.ua_date = ua_da
                    except NameError:
                        self.ua_date = datetime.strptime(ua_date['ua_date'], '%Y-%m-%d')
                        continue
        else:
            self.ua_date = None


def _find_target_dates(df: pd.DataFrame, keywords: List[str]):
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
        if any(x in a_string for x in keywords):
            target_dates.append((row['de_date_filed'], row['dt_text'], row['de_seqno']))
    return target_dates


def _complaint_dismmised(case: CaseDates, target: pd.DataFrame) -> CaseDates:
    """
    Function to find the dismissal date for a complaint that led to an amended complaint case.

    """
    dis = target.query('dp_sub_type == "dismcmp"')
    dis.drop_duplicates(subset=['de_seqno'], inplace=True)
    if not dis.empty:
        for index, row in dis.iterrows():
            case.dismissal_dates_prior_to_screening.append(
                {'dis_date': row['de_date_filed'], 'dis_reason': row['dt_text']})
    return case


def _find_complaint(target: pd.DataFrame) -> CaseDates:
    """
    Function to find the complaint date for a case.  Utilizes keywords to find initiating documents based on events. If
    multiple events found, the first date is used as the complaint filing date.  Also looks to see if any amended
    complaints wer filed

    """
    case = CaseDates(caseid=target['de_caseid'].iloc[0])
    cmp = target.query('dp_type == "motion" and dp_sub_type == "2255" or dp_sub_type=="cmp" or dp_sub_type=="pwrithc" '
                       'or dp_sub_type == "ntcrem" or dp_sub_type == "emerinj" or dp_sub_type == "bkntc" '
                       'or dp_sub_type == "setagr" ')
    if not cmp.empty:
        case.complaint_docnum = cmp['de_document_num'].iloc[0].astype(int)
        case.complaint_docnum = f'[{int(case.complaint_docnum)}]'
        case.complaint_date = cmp['de_date_filed'].iloc[0]
    # was case transferred?
    case = _get_transfer_date(case, target)
    case = _get_screening_date(case, target)
    # was an amended complaint, 2255 motion filed?
    case = _get_amended_complaints(case, target)
    return case


def _get_transfer_date(case, target):
    trf = target.query('dp_sub_type == "distin"')
    trf.drop_duplicates(subset='de_seqno', keep='first', inplace=True)
    if not trf.empty:
        case.transfer_date = trf['de_date_filed'].iloc[0]
    return case


def _get_amended_complaints(case, target):
    events = ['amdcmp', 'pamdcmp']
    mask = target['dp_sub_type'].isin(events)
    amdcmp = target[mask]
    if not amdcmp.empty:
        # case = _complaint_dismmised(case, target)
        amdcmp.drop_duplicates(subset='de_seqno', keep='first', inplace=True)
        for index, row in amdcmp.iterrows():
            amdcmp_docnum = row['de_document_num']
            amdcmp_docnum = f'[{int(amdcmp_docnum)}]'
            amdcmp_date = row['de_date_filed']
            if row['dp_dpseqno_ptr'] > 0:
                amdcmp_seqno_ptr = row['dp_dpseqno_ptr']
            else:
                amdcmp_seqno_ptr = 0
            case.amended_complaints.append({'amdcmp_date': amdcmp_date,
                                            'amdcmp_docnum': amdcmp_docnum,
                                            'amdcmp_seqno': row['dp_seqno'],
                                            'amdcmp_seqno_ptr': amdcmp_seqno_ptr})
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
        case_dates.ifp_date = ifp_date
    else:
        case_dates.ifp_docnum = '0'
    return case_dates


def _get_ua_date(case_dates, target):
    ua = target.loc[target['dp_sub_type'] == 'madv']
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


def _get_orders(case_dates, target):
    orders = target.loc[(target['dp_dpseqno_ptr'].notnull()) & (target['dp_type'] == 'order')]
    orders['dp_dpseqno_ptr'] = orders['dp_dpseqno_ptr'].astype(int)
    order = combine_docket_text_into_one_row(orders)
    order_motion = orders.merge(target, left_on='dp_dpseqno_ptr', right_on='dp_seqno', how='left')
    order_motion = order_motion.drop(['dp_seqno_x', 'dp_deseqno_ptr_x', 'dp_partno_x',
                                      'de_caseid_y', 'de_seqno_y', 'dp_dpseqno_ptr_y', 'dp_deseqno_ptr_y',
                                      'dp_partno_y', 'dp_dispositive_y', 'dp_action_type_y'], axis=1)

    order_motion.rename(columns={'de_caseid_x': 'Case ID',
                                 'de_type_x': 'Order Type',
                                 'dp_type_x': 'Order DP Type',
                                 'dp_sub_type_x': 'Order DP Sub Type',
                                 'de_seqno_x': 'Order Seqno',
                                 'dp_dispositive_x': 'Order Dispositive',
                                 'de_document_num_x': 'Order Docnum',
                                 'dp_dpseqno_ptr_x': 'Order DP Seqno PTR Num',
                                 'de_date_filed_x': 'Order Date',
                                 'dp_action_type_x': 'Order Action',
                                 'dt_text_x': 'Order Text',
                                 'de_type_y': 'Motion Type',
                                 'dp_type_y': 'Motion DP Type',
                                 'dp_sub_type_y': 'Motion DP Sub Type',
                                 'de_document_num_y': 'Motion Docnum',
                                 'dp_seqno_y': 'Motion Seqno',
                                 'de_date_filed_y': 'Motion Date',
                                 'dt_text_y': 'Motion Text', }, inplace=True)
    case_dates.order_dates = list(order_motion.to_dict('records'))
    return case_dates


def _check_for_trust_fund_statement(case_dates, target):
    # Was the case dismissed due to lack of prisoner trust fund statement?
    mask = target['dp_sub_type'] == 'termpscs'
    term = target[mask]
    if not term.empty:
        term_date = term['de_date_filed'].iloc[0]
        case_dates.trust_fund_dates.append(term_date)
    return case_dates


def _early_dismissal(case_dates, target) -> CaseDates:
    """
    Function to check for voluntary dismissal or other dismissal prior to screening 
   
    :param case_dates: CaseDates object
    :param target: dataframe of events for a case

    """
    s1 = target.loc[target['dp_sub_type'] == 'voldism']
    s2 = target.loc[target['dp_sub_type'] == 'termcs']
    s3 = target.loc[target['dp_sub_type'] == 'termpscs']
    s4 = target.loc[target['dp_sub_type'] == 'dismcmp']
    dism = pd.concat([s1, s2, s3, s4])
    dism.drop_duplicates(subset='de_seqno', keep='first', inplace=True)
    # take the last date filed
    if not dism.empty:
        dism.sort_values(by=['de_date_filed'], inplace=True, ascending=True)
        for index, row in dism.iterrows():
            dis_date = dism['de_date_filed'].iloc[0]
            dis_reason = dism['dp_sub_type'].iloc[0]
            dis_text = dism['dt_text'].iloc[0]
            type = dism['dp_sub_type'].iloc[0]
            case_dates.early_dismissal_dates.append({'dis_date': dis_date, 'dis_reason': dis_reason,
                                                     'dis_text': dis_text, 'type': type})
    return case_dates


def _get_reopen_date(case_dates, target):
    # Check for reopen motion
    reopen = target.loc[target['dp_sub_type'] == 'ropncs']
    if not reopen.empty:
        reopen.sort_values(by=['de_date_filed'], inplace=True, ascending=True)
        for index, row in reopen.iterrows():
            case_dates.case_reopen_dates.append(row['de_date_filed'])
    return case_dates


def _get_leave_to_proceed(case_dates, target):
    ltp = target.loc[target['dp_sub_type'] == 'leave']
    if not ltp.empty:
        ltp = ltp.drop_duplicates(subset=['dp_seqno'])
        for index, row in ltp.iterrows():
            case_dates.order_to_leave_dates.append({'ltp_date': row['de_date_filed'], 'seqno': row['dp_seqno'],
                                                    'ltp_text': row['dt_text']})
    # check for alternatives
    else:
        ltp = target.loc[target['dp_type'] == 'order']
        matches = ["leave to proceed"]
        ltp_dates = _find_target_dates(ltp, matches)
        # remove duplicates
        ltp_dates = set(ltp_dates)
        if ltp_dates:
            for ltp_date in ltp_dates:
                case_dates.order_to_leave_dates.append({'ltp_date': ltp_date[0],
                                                        'ltp_text': ltp_date[1],
                                                        'seqno': ltp_date[2]})
    return case_dates


def _get_pretrial_conference_date(case_dates, target):
    # Check for pretrial conference minutes. Subtype 'minutes' is not needed for identification
    pretrial = target.loc[target['dp_sub_type'] == 'ptcnf']
    if not pretrial.empty:
        case_dates.initial_pretrial_conference_date = pretrial['de_date_filed'].iloc[0]
    return case_dates


def _find_preliminary_pretrial_conference_deadlines(target_dline: pd.DataFrame, target_hearing: pd.DataFrame) \
        -> CaseDeadlines:
    """
    Function to find preliminary pretrial conference deadlines. Uses the first set of deadlines found.


    """
    try:
        deadline = CaseDeadlines(caseid=target_dline['sd_caseid'].iloc[0])
        try:
            deadline.dispositve_deadline = target_dline['sd_dtset'][target_dline['sd_type'] == 'disp'].iloc[0]
        except IndexError:
            deadline.dispositve_deadline = pd.NaT

        # Todo: Explore way to to handle multiple deadline types comparisons. Currently if one is not found, an error
        #  is thrown since a datetime cannot be compared to none.
        #  fptcnf = target_hearing['sd_dtset'][target_hearing['sd_type'] == 'fptcnf'] or \
        #  target_hearing['sd_dtset'][target_hearing['sd_type'] == 'Tfptcnf'] does not work if one of the values is not
        #  found. This is because the comparison is done between a datetime and a none type.
        try:
            deadline.limine_deadline = target_dline['sd_dtset'][target_dline['sd_type'] == 'limine'].iloc[0]
        except IndexError:
            pass
        try:
            if deadline.limine_deadline is None:
                deadline.limine_deadline = target_dline['sd_dtset'][target_dline['sd_type'] == 'Plimine'].iloc[0]
        except IndexError:
            deadline.limine_deadline = pd.NaT

        try:
            deadline.fptcnf_date = target_hearing['sd_dtset'][target_hearing['sd_type'] == 'fptcnf'].iloc[0]
        except IndexError:
            pass
        try:
            if deadline.fptcnf_date is None:
                deadline.fptcnf_date = target_hearing['sd_dtset'][target_hearing['sd_type'] == 'Tfptcnf'].iloc[0]
        except (IndexError, ValueError):
            deadline.fptcnf_date = pd.NaT

        try:
            deadline.trial_date = target_hearing['sd_dtset'][target_hearing['sd_type'] == 'jst'].iloc[-1]
        except IndexError:
            deadline.trial_date = pd.NaT
        return deadline

    except IndexError:
        return None


def get_case_deadlines(case_id: int, target_dline: pd.DataFrame, target_hearing: pd.DataFrame,
                       case_type: str) -> pd.DataFrame:
    try:
        deadline_dates = []
        print(Fore.BLUE + f'Getting deadline dates for {case_id}...', flush=True)
        deadline_dates.append({'case_id': case_id})
        # locate preliminary pre trial conference
        deadline_dates = _find_preliminary_pretrial_conference_deadlines(target_dline, target_hearing)
        print(Fore.WHITE + f'Finished getting case deadlines for: {case_id}')
        deadline_dates_dict = deadline_dates.dict()
        if deadline_dates_dict:
            return deadline_dates_dict
        else:
            return None

    except AttributeError as e:
        print(Fore.RED + f'No dates found for {case_id}: {e}', flush=True)


def _get_judgment_date(case_dates, target):
    judgments = target.loc[target['dp_sub_type'] == 'jgm']
    if not judgments.empty:
        judgments.drop_duplicates(subset=['dp_seqno'], inplace=True)
        judgments.sort_values(by=['de_date_filed'], inplace=True, ascending=True)
        for index, row in judgments.iterrows():
            case_dates.judgment_dates.append({'judgment_date': row['de_date_filed'], 'seqno': row['dp_seqno'], \
                                              'judgment_text': row['dt_text']})
    return case_dates


def _get_motion_for_reconsideration(case_dates, target):
    recon = target.loc[((target['dp_type'] == 'motion') & (target['dp_sub_type'] == 'recon'))]
    if recon:
        recon.drop_duplicates(subset=['dp_seqno'], inplace=True)
        recon.sort_values(by=['de_date_filed'], inplace=True, ascending=True)
        for index, row in recon.iterrows():
            case_dates.motion_for_reconsideration.append(
                {'motiont_date': row['de_date_filed'], 'seqno': row['dp_seqno'], \
                 'motion_text': row['dt_text']})
    return case_dates


def _get_notice_of_appeal(case_dates, target):
    noa = target.loc[target['dp_sub_type'] == 'ntcapp']
    if not noa.empty:
        noa.drop_duplicates(subset=['dp_seqno'], inplace=True)
        noa.sort_values(by=['de_date_filed'], inplace=True, ascending=True)
        for index, row in noa.iterrows():
            case_dates.notice_of_appeal_date = datetime.strptime(row['de_date_filed'], '%Y-%m-%d')
    return case_dates


def get_case_milestone_dates(case_id: int, target: pd.DataFrame, case_type: str) -> pd.DataFrame:
    """
    Wrapper Function to get case milestone dates for a case. Milestones include:
    Complaint, Screening, Under Advisement, IFP Motion, Dismissal, Reopen, Leave to Proceed, Pretrial Conference,
    """
    case_dates = []
    print(Fore.BLUE + f'Getting ua dates for {case_id}...', flush=True)
    try:
        case_dates.append({'case_id': case_id})
        # locate complaints
        case_dates = _find_complaint(target)
        case_dates.case_type = case_type
        if case_dates.complaint_date or case_dates.screening_date:
            # check for ifp motion
            case_dates = _get_ifp_date(case_dates, target)
        case_dates = _early_dismissal(case_dates, target)
        case_dates = _get_ua_date(case_dates, target)
        # check for leave to proceed
        case_dates = _get_leave_to_proceed(case_dates, target)
        case_dates = _get_pretrial_conference_date(case_dates, target)
        case_dates = _get_reopen_date(case_dates, target)
        if len(case_dates.case_reopen_dates) >= 1:
            case_dates = _get_judgment_date(case_dates, target)
        # check for case re-openings
        case_dates = _get_notice_of_appeal(case_dates, target)
        print(Fore.WHITE + f'Finished getting case dates for: {case_id}')
        case_dates_dict = case_dates.dict()
        return case_dates_dict
    except IndexError as e:
        print(Fore.RED + f'No dates found for {case_id}: {e}', flush=True)


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
    with open('dataframes.pkl', 'rb') as f:
        dataframes_entries = dill.load(f)

    df_entries = create_dataframe_docket_entries(dataframes_entries)
    df_entries['dt_text'] = df_entries['dt_text'].str.lower()

    with open('dataframes_deadlines.pkl', 'rb') as f:
        dataframes_deadlines = dill.load(f)

    df_deadlines = create_dataframe_deadlines(dataframes_deadlines)

    with open('dataframes_hearings.pkl', 'rb') as f:
        dataframes_hearings = dill.load(f)

    df_hearings = create_dataframe_hearings(dataframes_hearings)

    # delete dataframes to free up memory
    del dataframes_entries, dataframes_deadlines, dataframes_hearings
    ua_dates = []
    deadline_dates = []
    cases = pd.read_csv('/Users/jwt/PycharmProjects/cpi_program/data_files/pro_se.csv')
    cases = cases.loc[cases['Group'] != 'Habeas Corpus']
    cases[['Date Filed', 'Date Terminated', 'DateAgg']] = cases[['Date Filed', 'Date Terminated', 'DateAgg']].apply(
        pd.to_datetime, yearfirst=True,
        dayfirst=False, errors='coerce')
    cases = cases.drop(columns=['Diversity Plaintiff', 'Diversity Defendant', 'IsProse'])
    cases = cases.loc[cases['Date Filed'] >= datetime(2018, 1, 1)]
    cases['Case ID'] = cases['Case ID'].astype(int)
    cases.drop_duplicates(keep='first', inplace=True)

    caseids = cases['Case ID'].tolist()

    # gather information for each case and find the ua dates and deadlines
    for caseid in caseids:
        case_type = cases.loc[cases['Case ID'] == caseid, 'Group'].iloc[0]
        target = get_docker_entries_for_case(caseid, df_entries)
        ua_dates.append(get_case_milestone_dates(caseid, target, case_type))
        target_dline = df_deadlines.loc[df_deadlines['sd_caseid'] == caseid]
        target_hearings = df_hearings.loc[df_hearings['sd_caseid'] == caseid]
        deadline_dates.append(get_case_deadlines(caseid, target_dline, target_hearings, case_type))

    # save objects to disk for later use
    with open('ua_dates.pkl', 'wb') as f:
        dill.dump(ua_dates, f)
    with open('deadline_dates.pkl', 'wb') as f:
        dill.dump(deadline_dates, f)

    deadline_dates = [x for x in deadline_dates if x is not None]
    ua_dates = [x for x in ua_dates if x is not None]
    ua_dates_df = pd.DataFrame(ua_dates)
    deadline_dates_df = pd.DataFrame(deadline_dates)
    df = create_merged_ua_dates_or_deadlines(cases, ua_dates_df)
    df = create_merged_ua_dates_or_deadlines(df, deadline_dates_df)
    df = cleanup_merged_deadlines(df)
    # df = calculate_intervals(df)

    with open('case_metrics.pkl', 'wb') as f:
        dill.dump(df, f)


if __name__ == '__main__':
    main()
