import datetime

import pytest
import pandas as pd
import dill

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
    return df_entries


@pytest.fixture(scope='session')
def docket_deadlines():
    dataframes_deadlines = _create_dataframe('./data/dataframes_deadlines_testdata.pkl')
    df_deadlines = create_dataframe_deadlines(dataframes_deadlines)
    return df_deadlines


@pytest.fixture(scope='session')
def docket_hearings():
    dataframes_hearings = _create_dataframe('./data/dataframes_hearings_testdata.pkl')
    df_hearings = create_dataframe_hearings(dataframes_hearings)
    return df_hearings


@pytest.fixture(scope='function', params=[(41669, datetime.date(2018, 4, 20)),
                                          (41648, datetime.date(2018, 4, 19)),
                                          ])
def target_case(request):
    return request.param


# #
# # @pytest.fixture(params=[datetime.date(2018, 4, 20),
# #                         datetime.date(2018, 4, 19)
# #                         ])
# # def target_complaint_date(request):
# #     return request.param
#
#
# @pytest.fixture
# def case(target_case):
#     return CaseDates(caseid=target_case[0], case_type='Prisoner Petition', complaint_date=target_case[1])
#
#
# @pytest.fixture(params=[41669, 41648])
# def caseid(request):
#     return request.param
#
#
# @pytest.fixture()
# def docket_entries_for_case(docket_entries, caseid):
#     docket_entries_for_case = docket_entries.loc[docket_entries['de_caseid'] == caseid]
#     return docket_entries_for_case


@pytest.fixture()
def ua_dates(docket_entries, docket_deadlines, docket_hearings):
    ua_dates = CaseDates(docket_entries, docket_deadlines, docket_hearings)
    return ua_dates
