import datetime

import pytest
import pandas as pd
import dill

from ua_dates import _get_amended_complaints, _early_dismissal, _get_transfer_date, _get_ua_date, \
    _get_leave_to_proceed, _get_pretrial_conference_date, _get_reopen_date, _get_judgment_date, _find_complaint
from services.dataframe_services import create_dataframe_docket_entries, create_dataframe_deadlines, \
    create_dataframe_hearings
from ua_dates import CaseDeadlines, CaseDates


def _create_dataframe(path):
    with open(path, 'rb') as f:
        df = dill.load(f)
    return df


@pytest.fixture(scope='session')
def docket_entries():
    dataframes_entries = _create_dataframe('./data/dataframes_testdata.pkl')
    df_entries = create_dataframe_docket_entries(dataframes_entries)
    yield df_entries
    del df_entries


@pytest.fixture(scope='session')
def docket_deadlines():
    dataframes_deadlines = _create_dataframe('./data/dataframes_deadlines_testdata.pkl')
    df_deadlines = create_dataframe_deadlines(dataframes_deadlines)
    yield df_deadlines
    del df_deadlines


@pytest.fixture(scope='session')
def docket_hearings():
    dataframes_hearings = _create_dataframe('./data/dataframes_hearings_testdata.pkl')
    df_hearings = create_dataframe_hearings(dataframes_hearings)
    yield df_hearings
    del df_hearings


@pytest.fixture()
def _get_docket_entries_for_case(docket_entries, caseid):
    docket_entries_for_case = docket_entries.loc[docket_entries['de_caseid'] == caseid]
    return docket_entries_for_case


@pytest.fixture()
def create_case():
    def _create_case(caseid, complaint_date):
        return CaseDates(caseid=caseid, case_type='Prisoner Petition', complaint_date=complaint_date)

    return _create_case


@pytest.fixture()
def get_case_data():
    def _get_case_data(case, docket_entries_for_case):
        case = _get_amended_complaints(case, docket_entries_for_case)
        case = _early_dismissal(case, docket_entries_for_case)
        case = _get_transfer_date(case, docket_entries_for_case)
        case = _get_ua_date(case, docket_entries_for_case)
        case = _get_leave_to_proceed(case, docket_entries_for_case)
        case = _get_pretrial_conference_date(case, docket_entries_for_case)
        case = _get_reopen_date(case, docket_entries_for_case)
        case = _get_judgment_date(case, docket_entries_for_case)
        return case

    return _get_case_data
