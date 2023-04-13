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
    df = df.drop(['Diversity Defendant', 'Diversity Plaintiff', 'IsProse', 'caseid_x', 'caseid_y', 'case_type'], axis=1)
    df = df.drop_duplicates(keep='first')
    df['amended_complaint_count'] = df['amended_complaint_count'].fillna(0).astype(int)
    df['case_reopen_count'] = df['case_reopen_count'].fillna(0).astype(int)


def add_nos_grouping(df: pd.DataFrame, table_name: Table):
    nos_lookup = nos_group_lookup(table_name)
    df["Group"] = df.NOS.apply(lambda x: apply_nos_grouping(x, 'NOS', nos_lookup))
    return df
