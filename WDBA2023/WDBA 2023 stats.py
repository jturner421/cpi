import datetime
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
from pandas import Timestamp as ts
from configuration.config import Config
from db.dbsession import get_postgres_db_session
from services.api_services import ApiSession, get_data
from services.db_services import get_reflected_tables
from services.case_services import get_civil_cases_by_date, create_event
from services.dataframe_services import create_merged_df, add_nos_grouping
import matplotlib.style as style
import seaborn as sns


def identify_year(row):
    if pd.isnull(row['Date Reopened']):
        if row['Date Filed'] < datetime.date(2018, 7, 1):
            return 'earlier than 2018'
        elif datetime.date(2018, 7, 1) <= row['Date Filed'] <= datetime.date(2019, 6, 30):
            return '2019'
        elif datetime.date(2019, 7, 1) <= row['Date Filed'] <= datetime.date(2020, 6, 30):
            return '2020'
        elif datetime.date(2020, 7, 1) <= row['Date Filed'] <= datetime.date(2021, 6, 30):
            return '2021'
        elif datetime.date(2021, 7, 1) <= row['Date Filed'] <= datetime.date(2022, 6, 30):
            return '2022'
        elif datetime.date(2022, 7, 1) <= row['Date Filed'] <= datetime.date(2023, 6, 30):
            return '2023'
    elif row['Date Reopened'] and row['Case Year'] < 18:
        if row['Date Reopened'] < datetime.date(2018, 7, 1):
            return ' earlier than 2018'
        elif datetime.date(2018, 7, 1) <= row['Date Reopened'] <= datetime.date(2019, 6, 30):
            return '2019'
        elif datetime.date(2019, 7, 1) <= row['Date Reopened'] <= datetime.date(2020, 6, 30):
            return '2020'
        elif datetime.date(2020, 7, 1) <= row['Date Reopened'] <= datetime.date(2021, 6, 30):
            return '2021'
        elif datetime.date(2021, 7, 1) <= row['Date Reopened'] <= datetime.date(2022, 6, 30):
            return '2022'
        elif datetime.date(2022, 7, 1) <= row['Date Reopened'] <= datetime.date(2023, 6, 30):
            return '2023'
    else:
        if row['Date Filed'] < datetime.date(2018, 7, 1):
            return 'earlier than 2018'
        elif datetime.date(2018, 7, 1) <= row['Date Filed'] <= datetime.date(2019, 6, 30):
            return '2019'
        elif datetime.date(2019, 7, 1) <= row['Date Filed'] <= datetime.date(2020, 6, 30):
            return '2020'
        elif datetime.date(2020, 7, 1) <= row['Date Filed'] <= datetime.date(2021, 6, 30):
            return '2021'
        elif datetime.date(2021, 7, 1) <= row['Date Filed'] <= datetime.date(2022, 6, 30):
            return '2022'
        elif datetime.date(2022, 7, 1) <= row['Date Filed'] < datetime.date(2023, 6, 30):
            return '2023'


def identify_year_special(row):
    if pd.isnull(row['Date Reopened']):
        if row['Date Filed'] < datetime.date(2018, 4, 1):
            return 'earlier than 2018'
        elif row['Date Filed'] > datetime.date(2023, 3, 31):
            return 'later than 2023'
        elif datetime.date(2018, 4, 1) <= row['Date Filed'] <= datetime.date(2019, 3, 31):
            return '2019'
        elif datetime.date(2019, 4, 1) <= row['Date Filed'] <= datetime.date(2020, 3, 31):
            return '2020'
        elif datetime.date(2020, 4, 1) <= row['Date Filed'] <= datetime.date(2021, 3, 31):
            return '2021'
        elif datetime.date(2021, 4, 1) <= row['Date Filed'] <= datetime.date(2022, 3, 31):
            return '2022'
        elif datetime.date(2022, 4, 1) <= row['Date Filed'] <= datetime.date(2023, 3, 31):
            return '2023'

    elif row['Date Reopened'] and row['Case Year'] < 18:
        if row['Date Reopened'] < datetime.date(2018, 4, 1):
            return 'earlier than 2018'
        elif datetime.date(2018, 4, 1) <= row['Date Reopened'] <= datetime.date(2019, 3, 31):
            return '2019'
        elif datetime.date(2019, 4, 1) <= row['Date Reopened'] <= datetime.date(2020, 3, 31):
            return '2020'
        elif datetime.date(2020, 4, 1) <= row['Date Reopened'] <= datetime.date(2021, 3, 31):
            return '2021'
        elif datetime.date(2021, 4, 1) <= row['Date Reopened'] <= datetime.date(2022, 3, 31):
            return '2022'
        elif datetime.date(2022, 4, 1) <= row['Date Reopened'] <= datetime.date(2023, 3, 31):
            return '2023'
    else:
        if row['Date Filed'] < datetime.date(2021, 4, 1):
            return 'earlier than 2018'
        elif row['Date Filed'] > datetime.date(2023, 3, 31):
            return 'later than 2023'
        elif datetime.date(2018, 4, 1) <= row['Date Filed'] <= datetime.date(2019, 3, 31):
            return '2019'
        elif datetime.date(2019, 4, 1) <= row['Date Filed'] <= datetime.date(2020, 3, 31):
            return '2020'
        elif datetime.date(2020, 4, 1) <= row['Date Filed'] <= datetime.date(2021, 3, 31):
            return '2021'
        elif datetime.date(2021, 4, 1) <= row['Date Filed'] <= datetime.date(2022, 3, 31):
            return '2022'
        elif datetime.date(2022, 4, 1) <= row['Date Filed'] <= datetime.date(2023, 3, 31):
            return '2023'


def annualized_projection(value, num_months):
    return int(round(value * (12 / num_months), 0))


def main():
    get_postgres_db_session()
    nos, deadlines = get_reflected_tables()
    start_date = datetime.date(2018, 4, 1)
    end_date = datetime.date(2023, 3, 31)
    today = datetime.date.today()
    output_dir = Path.cwd() / 'WDBA2023' / 'data_files'
    output_raw_file = output_dir / f'wdba_stats_raw_{today:%b-%d-%y}.csv'
    output_processed_file = output_dir / f'wdba_stats_processed_{today:%b-%d-%y}.csv'
    stats = get_civil_cases_by_date(start_date=start_date, end_date=end_date)
    stats.to_csv(output_raw_file, index=False)
    stats = add_nos_grouping(stats, nos)
    # pro se versus civil counts
    stats['is_prose'] = stats['IsProse'].apply(lambda x: 'prose' if x == 'y' else 'counseled')

    stats['statistical_year'] = stats.apply(identify_year_special, axis=1)
    stats = stats[stats['statistical_year'] != 'earlier than 2018']
    stats = stats[stats['statistical_year'] != 'later than 2023']
    stats.drop(columns=['Case ID', 'Case Year', 'Reopen Code', 'Cause of Action', 'Diversity Plaintiff',
                        'Diversity Defendant', 'DateAgg', ], inplace=True)
    stats.rename(columns={'Case Number': 'case_number', 'Judge': 'judge', 'Date Filed': 'date_filed',
                          'Date Terminated': 'date_terminated', 'Date Reopened': 'date_reopened', 'Group': 'group'},
                 inplace=True)
    # remove whitespace from judge, and group columns in the dataframe
    stats['case_number'] = stats['case_number'].str.strip()
    stats['judge'] = stats['judge'].str.strip()
    stats['group'] = stats['group'].str.strip()
    pd.pivot_table(stats, index=['statistical_year'], aggfunc='count', values=['case_number'])
    # change some column types to categories
    stats.loc[:, 'is_prose'] = stats['is_prose'].astype('category')
    stats.loc[:, 'group'] = stats['group'].astype('category')

    # save processed file
    stats.to_csv(output_processed_file, index=False)

    # total_cases = stats.groupby(['Case Year']).agg({'Case ID': 'count'})
    # total_cases.reset_index(inplace=True)
    # # project last year of data
    # total_cases['Case ID'].iloc[-1] = annualized_projection(total_cases['Case ID'].iloc[-1], 11)
    #
    # # using the above, create a bar graph that shows the number of cases filed each year
    # style.use('fivethirtyeight')
    # # Setting size of our plot
    # fig, ax = plt.subplots(figsize=(12, 8))
    # # Bar width
    # bar_width = 0.8
    # # Positions of the left bar-boundaries
    # bar_l = [i for i in range(len(total_cases['Case Year']))]
    # # Positions of the x-axis ticks (center of the bars as bar labels)
    # tick_pos = [i + (bar_width / 2) for i in bar_l]
    # # Create the total_cases graph
    # plt.bar(bar_l,
    #         # using the Case ID data
    #         total_cases['Case ID'],
    #         # set the width
    #         width=bar_width,
    #         # with the label Case Year
    #         label='Case Year',
    #         # with alpha 0.5
    #         alpha=0.5,
    #         # with color
    #         color='#0F95D7')
    # plt.tick_params(axis='both', which='major', labelsize=18)
    #
    # plt.ylim(-10, 1200)
    # # Generate a bolded horizontal line at y = 0
    # plt.axhline(y=0, color='#414141', linewidth=1.3, alpha=.5)
    # # Set the x ticks with names
    # plt.xticks(bar_l, total_cases['Case Year'])
    # ax.set_ylabel("Number of Cases", fontsize=18)
    #
    # # fig.suptitle('Civil Cases Filed by Statistical Year', fontsize=20, fontweight='bold')
    # # Title text
    # # plt.text(x=900.7, y=618, s="Who Got to Be On 'The Daily Show'?", fontsize=18.5, fontweight='semibold',
    # #         color='#414141')
    # plt.tight_layout()
    # plt.savefig('/Users/jwt/PycharmProjects/cpi_program/plots/civil_cases_by_year.png')
    # plt.show()
    #
    # # Set the label and legends
    # # ax.set_ylabel("Number of Cases", fontsize=18)
    # # ax.set_xlabel("Year", fontsize=18)
    # # Y axis past 0 & above 1000 -- grid line will pass 0 & 1000 marker
    # # Bolded horizontal line at y=0
    # ax.axhline(y=0, color='#414141', linewidth=1.5, alpha=.5)
    # # Rotate axis labels
    # # plt.setp(ax.get_xticklabels(), rotation=45, ha="right",
    # #          rotation_mode="anchor")
    # # # Title text
    # # plt.text(x=1996.7, y=118, s="Civil Cases by Year", fontsize=18.5, fontweight='semibold',
    # #          color='#414141')
    # # # Line at bottom for signature line
    # # plt.text(x=1996.7, y=-18.5,
    # #          s='   ©Western District of Wisconsin                                         Source: CM/ECF',
    # #          fontsize=14, color='#f0f0f0', backgroundcolor='#414141');
    # # plt.show()
    # #
    # # style.use('fivethirtyeight')
    # # graph = total_cases.plot.bar(x='Case Year', y='Case ID', legend=False,
    # #                              figsize=(12, 8))
    # # graph = total_cases.plot.bar(bar_l,
    # #                              # using the Case ID data
    # #                              total_cases['Case ID'],
    # #                              # set the width
    # #                              width=bar_width,
    # #                              # with the label Case Year
    # #                              label='Case Year',
    # #                              # with alpha 0.5
    # #                              alpha=0.5,
    # #                              # with color
    # #                              color='#0F95D7')
    # #
    # # graph.tick_params(axis='both', which='major', labelsize=18)
    # # graph.set_xlabel('Year', fontsize=18)
    # # graph.set_ylabel('Number of Cases', fontsize=18)
    # # graph.set_ylim(bottom=-10, top=1200)
    # # graph.axhline(y=0, color='black', linewidth=1.5, alpha=.5)
    # # graph.set_yticks([0, 200, 400, 600, 800, 1000, 1200])
    # # graph.set_yticklabels([0, 200, 400, 600, 800, 1000, 1200], fontsize=18, color='#414141')
    # #
    # # plt.show()
    # # graph.set_title('Total Number of Cases Filed Each Year', fontsize=24)
    #
    # # group by Case Year and provide value counts for is_prose
    # prose_vs_counseled = stats.groupby(['Case Year', 'is_prose', 'Group']).agg({'Case ID': 'count'})
    #
    # pro_se_breakdown = prose.groupby(['Case Year', 'Group']).agg({'Case ID': 'count'})
    # counseled_breakdown = counseled.groupby(['Case Year', 'Group']).agg({'Case ID': 'count'})


#     create a bar graph from counseled_breakdown that shows the number of cases grouped by year and group

if __name__ == '__main__':
    main()
