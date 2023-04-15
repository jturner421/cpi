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


@pytest.fixture(scope='module')
def docket_entries():
    dataframes_entries = _create_dataframe('../dataframes.pkl')
    df_entries = create_dataframe_docket_entries(dataframes_entries)
    return df_entries


@pytest.fixture(scope='module')
def docket_deadlines():
    dataframes_deadlines = _create_dataframe('dataframes_deadlines.pkl')
    df_deadlines = create_dataframe_deadlines(dataframes_deadlines)
    return df_deadlines


@pytest.fixture(scope='module')
def docket_hearings():
    dataframes_hearings = _create_dataframe('../dataframes_hearings.pkl')
    df_hearings = create_dataframe_hearings(dataframes_hearings)
    return df_hearings

