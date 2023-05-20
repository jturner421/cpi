import pandas as pd
import datetime
from configuration.config import Config
from db.dbsession import get_postgres_db_session
from services.api_services import ApiSession, get_data
from services.db_services import get_reflected_tables
from services.case_services import get_civil_cases_by_date, create_event
from services.dataframe_services import create_merged_df, add_nos_grouping

api = ApiSession.instance()
get_postgres_db_session()
config = Config()
api_base_url = config.base_api_url


def main():
    # reflection of PostgreSql database tables used for later processing
    nos, deadlines = get_reflected_tables()
    start_date = datetime.date(2020, 1, 1)
    end_date = datetime.date(2023, 5, 31)
    stats = get_civil_cases_by_date(start_date=start_date, end_date=end_date)
    stats = add_nos_grouping(stats, nos)
    stats.to_csv('/Users/jwt/PycharmProjects/cpi_program/data_files/civil_cases_2020-2023.csv', index=False)

    # filter dataframe to return cases where IsProse is y
    mask = stats['IsProse'] == 'y'
    pro_se = stats[mask]
    pro_se.to_csv('/Users/jwt/PycharmProjects/cpi_program/data_files/pro_se.csv', index=False)

    # retrieve complaints from ecf for cases
    url = f'{api_base_url}{config.docket_entries_by_case_and_type}'
    params = {'documents': False, 'docket_text': False}
    case_ids = pro_se['Case ID'].to_list()
    complaints = get_data(case_ids, api.access_token, url, params, event=None,
                          overall_type='cmp')
    complaints.to_csv('/Users/jwt/PycharmProjects/cpi_program/data_files/complaints.csv', index=False)

    # retrieve habeas complaints from ecf for cases
    url = f'{api_base_url}{config.docket_entries_by_case_and_typeSub}'
    event = create_event(('motion', 'pwrithc'))
    habeas_complaints = get_data(case_ids, api.access_token, url, params, event=event,
                                 overall_type=None)
    habeas_complaints.to_csv('/Users/jwt/PycharmProjects/cpi_program/data_files/habeas_complaints.csv', index=False)

    # retrieve 2255 motions from ecf for cases
    params = {'documents': False, 'docket_text': False}
    event = create_event(('motion', '2255'))
    motion_2255 = get_data(case_ids, api.access_token, url, params, event=event,
                           overall_type=None)
    motion_2255.to_csv('/Users/jwt/PycharmProjects/cpi_program/data_files/motion_2255.csv', index=False)

    # retrieve notice of removal from ecf for cases
    params = {'documents': False, 'docket_text': False}
    event = create_event(('notice', 'ntcrem'))
    notice_removal = get_data(case_ids, api.access_token, url, params, event=event,
                              overall_type=None)
    notice_removal.to_csv('/Users/jwt/PycharmProjects/cpi_program/data_files/notice_removal.csv', index=False)

    # retrieve emergency injunctions from ecf for cases

    params = {'documents': False, 'docket_text': False}
    event = create_event(('motion', 'emerinj'))
    injunctions = get_data(case_ids, api.access_token, url, params, event=event,
                           overall_type=None)
    injunctions.to_csv('/Users/jwt/PycharmProjects/cpi_program/data_files/injunctions.csv', index=False)

    # retrieve bankruptcy  appeals from ecf for cases
    params = {'documents': False, 'docket_text': False}
    event = create_event(('appeal', 'bkntc'))
    bk_appeal = get_data(case_ids, api.access_token, url, params, event=event,
                         overall_type=None)
    bk_appeal.to_csv('/Users/jwt/PycharmProjects/cpi_program/data_files/bk_appeal.csv', index=False)

    # merge complaint dataframes
    complaints_df = create_merged_df(pro_se, complaints)

    # habeas corpus complaints
    habeas_df = create_merged_df(pro_se, habeas_complaints)

    # 2255 motions
    df_2255 = create_merged_df(pro_se, motion_2255)

    # Notice of Removals
    notice_df = create_merged_df(pro_se, notice_removal)

    injunctions_df = create_merged_df(pro_se, injunctions)

    df = pd.concat(
        [complaints_df, habeas_df, df_2255, notice_df, injunctions_df], ignore_index=True)
    df.drop_duplicates(subset=['Case ID'], keep='first', inplace=True)
    df.to_csv('/Users/jwt/PycharmProjects/cpi_program/data_files/prose_merged.csv', index=False)


if __name__ == '__main__':
    main()
