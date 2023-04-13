from datetime import datetime
from dataclasses import dataclass, field
from enum import Enum
import dill
import pandas as pd
from colorama import Fore

from util import timeit
from services.dataframe_services import create_merged_ua_dates_or_deadlines, cleanup_merged_deadlines


class DismissalType(Enum):
    voldism = 'Voluntary Dismissal'
    termpscs = 'No Trust Fund Statement'
    termcs = 'Civil Case Terminated'


@dataclass
class CaseDeadlines:
    caseid: int
    pptcnf_date: datetime = None
    dispositve_deadline: datetime = None
    limine_deadline: datetime = None
    fptcnf_date: datetime = None

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
    amended_complaint_count: int = None
    case_reopen_count: int = None
    ifp_docnum: int = None
    ifp_date: datetime = None
    screening_docnum: str = "0"
    screening_date: datetime = None
    ua_date: datetime = None
    ltp_date: datetime = None
    initial_pretrial_conference_date: datetime = None
    dismissal_date: datetime = None
    dismissal_reason: str = None
    amended_complaints: list = field(default_factory=list)
    order_to_leave_dates: list = field(default_factory=list)
    case_reopen_dates: list = field(default_factory=list)
    ua_dates: list = field(default_factory=list)
    trust_fund_dates: list = field(default_factory=list)
    dismissal_dates: list = field(default_factory=list)

    def dict(self):
        if len(self.order_to_leave_dates) == 1:
            self.ltp_date = datetime.strptime(self.order_to_leave_dates[0]['ltp_date'], '%Y-%m-%d')
        elif len(self.order_to_leave_dates) > 2:
            print(f' More then one Leave order in {self.caseid}')
        else:
            self.ltp_date = None
            print(f' No Leave order in {self.caseid}')

        if len(self.ua_dates) >= 1:
            # assumption that first UA date is screening date
            self.ua_date = datetime.strptime(self.ua_dates[0]['ua_date'], '%Y-%m-%d')
        else:
            self.ua_date = None
            print(f' No UA date in {self.caseid}')
        if len(self.dismissal_dates):
            self.dismissal_date = datetime.strptime(self.dismissal_dates[-1]['dis_date'], '%Y-%m-%d')
            self.dismissal_reason = DismissalType[self.dismissal_dates[-1]['dis_reason']].value
        if self.dismissal_date is not None and self.ua_date is not None:
            if self.ua_date > self.dismissal_date:
                print(f'{Fore.RED} UA date after dismissal date in {self.caseid}{Fore.RESET}')
                self.ua_date = None

        if self.initial_pretrial_conference_date:
            self.initial_pretrial_conference_date = datetime.strptime(self.initial_pretrial_conference_date,
                                                                      '%Y-%m-%d')
        self.amended_complaint_count = len(self.amended_complaints)
        self.case_reopen_count = len(self.case_reopen_dates)
        _dict = self.__dict__.copy()
        if _dict['complaint_date'] is not None and _dict['complaint_date'] != 0:
            _dict['complaint_date'] = datetime.strptime(_dict['complaint_date'], '%Y-%m-%d')
        if _dict['ifp_date'] is not None and _dict['ifp_date'] != 0:
            _dict['ifp_date'] = datetime.strptime(_dict['ifp_date'], '%Y-%m-%d')
        if _dict['screening_date'] is not None and _dict['screening_date'] != 0:
            _dict['screening_date'] = datetime.strptime(_dict['screening_date'], '%Y-%m-%d')
        del _dict['amended_complaints']
        del _dict['order_to_leave_dates']
        del _dict['ua_dates']
        del _dict['trust_fund_dates']
        del _dict['dismissal_dates']
        del _dict['complaint_docnum']
        del _dict['screening_docnum']
        del _dict['ifp_docnum']
        del _dict['case_reopen_dates']
        return _dict


def find_ua_date(cmp_docnum, screening_docnum, ua):
    ua['dt_text'] = ua['dt_text'].str.lower()
    ua_dates = []
    matches = [cmp_docnum, str(screening_docnum), "screening", 'complaint', 'pauperis', 'habeas', 'reopen',
               'social security', 'bankruptcy', 'prepayment']
    for index, row in ua.iterrows():
        a_string = row['dt_text']
        if any(x in a_string for x in matches):
            ua_dates.append((row['de_date_filed'], row['dt_text']))
        # else:
        #     ua_date = np.nan
    return ua_dates


def _find_complaint(target):
    case = CaseDates(caseid=target['de_caseid'].iloc[0])
    cmp = target.query('dp_type == "motion" and dp_sub_type == "2255" or dp_sub_type=="cmp" or dp_sub_type=="pwrithc" '
                       'or dp_sub_type == "ntcrem" or dp_sub_type == "emerinj" or dp_sub_type == "bkntc" '
                       'or dp_sub_type == "setagr" ')
    if not cmp.empty:
        case.complaint_docnum = cmp['de_document_num'].iloc[0].astype(int)
        case.complaint_docnum = f'[{int(case.complaint_docnum)}]'
        case.complaint_date = cmp['de_date_filed'].iloc[0]
    # check for amended complaints

    case = _get_screening_date(case, target)
    # was an amended complaint, 2255 motion filed?
    events = ['amdcmp', 'pamdcmp']
    mask = target['dp_sub_type'].isin(events)
    amdcmp = target[mask]
    if not amdcmp.empty:
        amdcmp_docnum = amdcmp['de_document_num'].iloc[0].astype(int)
        amdcmp_docnum = f'[{int(amdcmp_docnum)}]'
        amdcmp_date = amdcmp['de_date_filed'].iloc[0]
        case.amended_complaints.append({'amdcmp_date': amdcmp_date,
                                        'amdcmp_docnum': amdcmp_docnum})
    return case


def _get_screening_date(case, target):
    # Check for dummy screening motion
    screening = target.loc[target['dp_sub_type'] == 'dummyscr']
    if not screening.empty:
        case.screening_docnum = screening['de_document_num'].iloc[0].astype(int)
        case.screening_docnum = f'[{int(case.screening_docnum)}]'
        case.screening_date = screening['de_date_filed'].iloc[0]

    else:
        case.screening_docnum = '0'
    return case


def _get_ifp_date(case_dates, target):
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


def _get_ua_date(case_dates, ua):
    if not ua.empty:
        ua_dates = find_ua_date(case_dates.complaint_docnum, case_dates.screening_docnum, ua)
        if ua_dates:
            for ua_date in ua_dates:
                case_dates.ua_dates.append({'ua_date': ua_date[0], 'ua_text': ua_date[1]})
    return case_dates


def _check_for_trust_fund_statement(case_dates, target):
    # Was the case dismissed due to lack or prisoner trust fund statement?
    mask = target['dp_sub_type'] == 'termpscs'
    term = target[mask]
    if not term.empty:
        term_date = term['de_date_filed'].iloc[0]
        case_dates.trust_fund_dates.append(term_date)
    return case_dates


def _early_dismissal(case_dates, target):
    s1 = target.loc[target['dp_sub_type'] == 'voldism']
    s2 = target.loc[target['dp_sub_type'] == 'termcs']
    s3 = target.loc[target['dp_sub_type'] == 'termpscs']
    dism = pd.concat([s1, s2, s3])
    # take the last date filed
    if not dism.empty:
        dism.sort_values(by=['de_date_filed'], inplace=True, ascending=True)
        dis_date = dism['de_date_filed'].iloc[-1]
        dis_reason = dism['dp_sub_type'].iloc[-1]
        case_dates.dismissal_dates.append({'dis_date': dis_date, 'dis_reason': dis_reason})
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
            case_dates.order_to_leave_dates.append({'ltp_date': row['de_date_filed'], 'seqno': row['dp_seqno']})
    else:
        case_dates = _early_dismissal(case_dates, target)
    return case_dates


def _get_pretrial_conference_date(case_dates, target):
    # Check for pretrial conference
    pretrial = target.loc[target['dp_sub_type'] == 'ptcnf']
    if not pretrial.empty:
        for index, row in pretrial.iterrows():
            case_dates.initial_pretrial_conference_date = row['de_date_filed']
    return case_dates


def _find_preliminary_pretrial_conference(target_dline, target_hearing):
    try:
        deadline = CaseDeadlines(caseid=target_dline['sd_caseid'].iloc[0])
        ptcnf = target_hearing.query('sd_type == "ptcnf" and sd_class == "hrg" ')
        if not ptcnf.empty:
            deadline.pptcnf_date = ptcnf['sd_dtset'].iloc[0]
            deadline.dispositve_deadline = target_dline['sd_dtset'][target_dline['sd_type'] == 'disp'].iloc[-1]
            deadline.limine_deadline = target_dline['sd_dtset'][target_dline['sd_type'] == 'limine'].iloc[-1]
            deadline.fptcnf_date = target_hearing['sd_dtset'][target_hearing['sd_type'] == 'fptcnf'].iloc[-1]
        return deadline
    except Exception as e:
        print(Fore.RED + f'No Deadlines Found: {e}', flush=True)
        return None


def get_case_deadlines(case_id: int, target_dline: pd.DataFrame, target_hearing: pd.DataFrame,
                       case_type: str) -> pd.DataFrame:
    try:
        deadline_dates = []
        print(Fore.BLUE + f'Getting deadline dates for {case_id}...', flush=True)
        deadline_dates.append({'case_id': case_id})
        # locate preliminary pre trial conference
        deadline_dates = _find_preliminary_pretrial_conference(target_dline, target_hearing)
        print(Fore.WHITE + f'Finished getting case deadlines for: {case_id}')
        deadline_dates_dict = deadline_dates.dict()
        if deadline_dates_dict:
            return deadline_dates_dict
        else:
            return None

    except AttributeError as e:
        print(Fore.RED + f'No dates found for {case_id}: {e}', flush=True)


def get_case_milestone_dates(case_id: int, target: pd.DataFrame, case_type: str) -> pd.DataFrame:
    """
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
        ua = target.loc[target['dp_sub_type'] == 'madv']

        if not ua.empty:
            # Find the under advisement date for the complaint
            case_dates = _get_ua_date(case_dates, ua)
            if not case_dates.ua_dates:
                case_dates = _check_for_trust_fund_statement(case_dates, target)
        # Was there a voluntary dismissal or termination due to no prisoner trust fund statement?
        case_dates = _early_dismissal(case_dates, target)
        # check for leave to proceed
        case_dates = _get_leave_to_proceed(case_dates, target)
        case_dates = _get_pretrial_conference_date(case_dates, target)
        # check for case re-openings
        case_dates = _get_reopen_date(case_dates, target)

        print(Fore.WHITE + f'Finished getting case dates for: {case_id}')
        case_dates_dict = case_dates.dict()
        return case_dates_dict
    except IndexError as e:
        print(Fore.RED + f'No dates found for {case_id}: {e}', flush=True)


@timeit
def main():
    # get saved docket entries
    with open('dataframes.pkl', 'rb') as f:
        dataframes_entries = dill.load(f)

    df_entries = pd.concat(dataframes_entries)
    # cleanup to assist with string matching and to eliminate unnecessary columns
    df_entries = df_entries.drop(['de_date_enter', 'de_who_entered', 'initials', 'name', 'pr_type', 'pr_crttype'],
                                 axis=1)
    df_entries['de_type'] = df_entries['de_type'].str.strip()
    df_entries['dp_type'] = df_entries['dp_type'].str.strip()
    df_entries['dp_sub_type'] = df_entries['dp_sub_type'].str.strip()
    del dataframes_entries

    with open('dataframes_deadlines.pkl', 'rb') as f:
        dataframes_deadlines = dill.load(f)

    df_deadlines = pd.concat(dataframes_deadlines)
    # change column string to datetime
    df_deadlines['sd_dtset'] = pd.to_datetime(df_deadlines['sd_dtset'], dayfirst=False, yearfirst=True, errors='coerce')
    df_deadlines['sd_dtsatis'] = pd.to_datetime(df_deadlines['sd_dtsatis'], dayfirst=False, yearfirst=True,
                                                errors='coerce')
    df_deadlines['sd_dtsatis'] = pd.to_datetime(df_deadlines['sd_dtsatis'], dayfirst=False, yearfirst=True,
                                                errors='coerce')
    df_deadlines['sd_class'] = df_deadlines['sd_class'].str.strip()
    df_deadlines['sd_type'] = df_deadlines['sd_type'].str.strip()

    del dataframes_deadlines

    with open('dataframes_hearings.pkl', 'rb') as f:
        dataframes_hearings = dill.load(f)

    df_hearings = pd.concat(dataframes_hearings)
    # change column string to datetime
    df_hearings['sd_dtset'] = pd.to_datetime(df_hearings['sd_dtset'], dayfirst=False, yearfirst=True, errors='coerce')
    df_hearings['sd_dtsatis'] = pd.to_datetime(df_hearings['sd_dtsatis'], dayfirst=False, yearfirst=True,
                                               errors='coerce')
    df_hearings['sd_dtsatis'] = pd.to_datetime(df_hearings['sd_dtsatis'], dayfirst=False, yearfirst=True,
                                               errors='coerce')
    df_hearings['sd_class'] = df_hearings['sd_class'].str.strip()
    df_hearings['sd_type'] = df_hearings['sd_type'].str.strip()

    ua_dates = []
    deadline_dates = []
    cases = pd.read_csv('/Users/jwt/PycharmProjects/cpi_program/data_files/pro_se.csv')
    cases[['Date Filed', 'Date Terminated', 'DateAgg']] = cases[['Date Filed', 'Date Terminated', 'DateAgg']].apply(
        pd.to_datetime, yearfirst=True,
        dayfirst=False, errors='coerce')
    cases['Case ID'] = cases['Case ID'].astype(int)
    caseids = cases['Case ID'].tolist()

    # gather information for each case and find the ua dates and deadlines
    for caseid in caseids:
        case_type = cases.loc[cases['Case ID'] == caseid, 'Group'].iloc[0]
        target = df_entries.loc[df_entries['de_caseid'] == caseid]
        ua_dates.append(get_case_milestone_dates(caseid, target, case_type))
        target_dline = df_deadlines.loc[df_deadlines['sd_caseid'] == caseid]
        target_hearings = df_hearings.loc[df_hearings['sd_caseid'] == caseid]
        deadline_dates.append(get_case_deadlines(caseid, target_dline, target_hearings, case_type))

    with open('ua_dates.pkl', 'wb') as f:
        dill.dump(ua_dates, f)

    deadline_dates = [x for x in deadline_dates if x is not None]
    ua_dates = [x for x in ua_dates if x is not None]
    ua_dates_df = pd.DataFrame(ua_dates)
    deadline_dates_df = pd.DataFrame(deadline_dates)
    df = create_merged_ua_dates_or_deadlines(cases, ua_dates_df)
    df = create_merged_ua_dates_or_deadlines(df, deadline_dates_df)
    df = cleanup_merged_deadlines(df)
    # with open('deadline_dates.pkl', 'wb') as f:
    #     dill.dump(deadline_dates, f)

    # with open('ua_dates.pkl', 'rb') as f:
    #     ua_dates = dill.load(f)
    # for case in ua_dates:
    #     if len()
    #     print(case)


if __name__ == '__main__':
    main()
