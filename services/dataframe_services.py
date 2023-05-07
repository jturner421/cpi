from typing import List, Dict

import pandas as pd
from sqlalchemy import Table

from services.db_services import nos_group_lookup


def _format_columns(df) -> pd.DataFrame:
    """
    Formats and renames columns of civil case dataframe

    :param df: dataframe to be formatted
    :return: formatted dataframe

    """
    df['cs_date_filed'] = pd.to_datetime(df['cs_date_filed'], format='%Y-%m-%d')
    df['cs_date_term'] = pd.to_datetime(df['cs_date_term'], format='%Y-%m-%d')
    df['cs_date_reopen'] = pd.to_datetime(df['cs_date_reopen'], format='%Y-%m-%d')
    df['j5_nature_of_suit'] = df['j5_nature_of_suit'].astype('int')
    df.columns = ['Case ID', 'Case Number', 'Judge', 'Date Filed', 'Date Reopened',
                  'Date Terminated',
                  'NOS',
                  'Cause of Action', 'Diversity Plaintiff', 'Diversity Defendant', 'IsProse']

    # Creates new column used for visualizations. For reopened cases, sets the filing date to the reopened date.
    df['DateAgg'] = df.apply(
        lambda x: x['Date Reopened'] if x['Date Reopened'] > x['Date Filed'] else x['Date Filed'], axis=1)
    return df


def apply_nos_grouping(row: pd.Series, column_name: str, nos_lookup: List[Dict]) -> str:
    """
    Applies nature of suit grouping based on Civil Nature of Suit Code Descriptions found on the JNET

    Usage:
    df.apply(lambda x: apply_nos_grouping(x, 'NOS', nos_lookup), axis=1)

    Dataframe apply functions act against the entire table and serve as a replacement for a for loop. It is common to
    use an anonymous function to pass dataframe objects for the function to act upon. The argument axis = 1 insures that
    the function is applied on a row by row basis as opposed to column basis

    :param row: Dataframe row passed as a series
    :param column_name Dataframe column that holds nature of suit code
    :param nos_lookup: List of Dicts {NOS_code: Grouping Description} e.g. [{'110': 'Contract'}]
    :return: grouping value
    """

    try:
        # find index of nature of suit match from dataframe row in list of dicts
        nos_group = next((i, d) for i, d in enumerate(nos_lookup) if row in d)
        return nos_group[1][row]
    except StopIteration:
        print(row)


def create_merged_df(original_df, candidate_df) -> pd.DataFrame:
    """
    Merges two dataframes on case ID and drops unnecessary columns

    :param original_df: original dataframe
    :param candidate_df: dataframe to be merged

    :return: merged dataframe

    """
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


def create_merged_ua_dates_or_deadlines(original_df, candidate_df) -> pd.DataFrame:
    df = pd.merge(original_df, candidate_df, left_on='Case ID', right_on='caseid', how='left')
    return df


def cleanup_merged_deadlines(df) -> pd.DataFrame:
    # df = df.drop(['Diversity Defendant', 'Diversity Plaintiff', 'IsProse', 'caseid_x', 'caseid_y', 'case_type'], axis=1)
    df = df.drop(['caseid_x', 'caseid_y', 'case_type'], axis=1)
    df['amended_complaint_count'] = df['amended_complaint_count'].fillna(0).astype(int)
    df['case_reopen_count'] = df['case_reopen_count'].fillna(0).astype(int)
    return df


def calculate_intervals(df) -> pd.DataFrame:
    """
    Calculates various intervals between case milestone dates

    :param df: dataframe with raw data
    :return: dataframe with calculated intervals

    """
    df['CMP To LTP Elapsed'] = df['ltp_date'] - df['complaint_date']
    df['CMP To UA Elapsed'] = df['ua_date'] - df['complaint_date']
    df['UA To LTP Elapsed'] = df['ltp_date'] - df['ua_date']
    df['LTP to PPTCNF Elapsed'] = df['initial_pretrial_conference_date'] - df['ltp_date']
    df['PPTCNF to SJ Elapsed'] = df['dispositive_deadline'] - df['initial_pretrial_conference_date']
    df['SJ to FPTCNF Elapsed'] = df['fptcnf_date'] - df['dispositive_deadline']
    return df


def add_nos_grouping(df: pd.DataFrame, table_name: Table):
    nos_lookup = nos_group_lookup(table_name)
    df["Group"] = df.NOS.apply(lambda x: apply_nos_grouping(x, 'NOS', nos_lookup))
    return df


def create_dataframe_docket_entries(dataframes_entries: List[Dict]) -> pd.DataFrame:
    """
    Concatenates list of dictionaries into a single dataframe with case docket entries with some formatting and cleanup.

    :param dataframes_entries: list of dictionaries
    :return: dataframe
    """
    df_entries = pd.concat(dataframes_entries)
    # cleanup to assist with string matching and to eliminate unnecessary columns
    df_entries = df_entries.drop(['de_date_enter', 'de_who_entered', 'initials', 'name', 'pr_type', 'pr_crttype'],
                                 axis=1)
    df_entries['de_type'] = df_entries['de_type'].str.strip()
    df_entries['dp_type'] = df_entries['dp_type'].str.strip()
    df_entries['dp_sub_type'] = df_entries['dp_sub_type'].str.strip()
    del dataframes_entries
    return df_entries


def create_dataframe_deadlines(dataframes_deadlines):
    """
        Concatenates list of dictionaries into a single dataframe of case deadlines with some formatting and cleanup.

        :param dataframes_deadlines: list of dictionaries
        :return: dataframe
        """
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
    return df_deadlines


def create_dataframe_hearings(dataframes_hearings):
    """
            Concatenates list of dictionaries into a single dataframe of case hearings with some formatting and cleanup.

            :param dataframes_hearings: list of dictionaries
            :return: dataframe
            """
    df_hearings = pd.concat(dataframes_hearings)
    # change column string to datetime
    df_hearings['sd_dtset'] = pd.to_datetime(df_hearings['sd_dtset'], dayfirst=False, yearfirst=True, errors='coerce')
    df_hearings['sd_dtsatis'] = pd.to_datetime(df_hearings['sd_dtsatis'], dayfirst=False, yearfirst=True,
                                               errors='coerce')
    df_hearings['sd_dtsatis'] = pd.to_datetime(df_hearings['sd_dtsatis'], dayfirst=False, yearfirst=True,
                                               errors='coerce')
    df_hearings['sd_class'] = df_hearings['sd_class'].str.strip()
    df_hearings['sd_type'] = df_hearings['sd_type'].str.strip()
    return df_hearings
