import datetime

import pandas as pd

from configuration.config import Config
from services.dataframe_services import _format_columns
from services.api_services import ApiSession

api = ApiSession.instance()
def get_civil_cases_by_date(start_date: datetime.date, end_date: datetime.date) -> pd.DataFrame:
    """
    retrieves civil cases between two dates

    :param start_date: start date of case retrieval
    :param end_date: end date of case retrieval
    :return:
    """
    config = Config()
    date_format = "%Y-%m-%d"
    end_date_string = end_date.strftime(date_format)
    start_date_string = start_date.strftime(date_format)
    api_base_url = config.base_api_url
    cases_url = config.civil_cases_endpoint
    url = f'{api_base_url}{cases_url}'
    payload = {
        "start_date": start_date_string,
        "end_date": end_date_string
    }
    # headers = {'Authorization': f'Bearer {api.headers}'}
    df = api.get(url, payload)
    df = _format_columns(df)
    return df


def create_event(event_type: tuple) -> list:
    """
    Formats event type to be used for API call

    :param event_type: tuple of event types e.g. ('cmp','cmp')
    :return: list of event types e.g. [['cmp','cmp']]

    """
    event = []
    stage_list = []
    for e in event_type:
        stage_list.append(e)
    event.append(stage_list)
    return event


