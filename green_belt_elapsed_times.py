import pandas as pd
import numpy as np
import dill
import openpyxl

with open('data_files/green_belt_dates.pkl', 'rb') as f:
    df = dill.load(f)


def _calculate_total_elapsed_time(complaint_date, transfer_date, ua_date, dismissal_date, full_fee_paid,
                                  partial_payment_date):
    if full_fee_paid:
        total_elapsed_time = (partial_payment_date - complaint_date).days
    elif dismissal_date:
        total_elapsed_time = (pd.to_datetime(dismissal_date) - complaint_date).days
    elif transfer_date > complaint_date:
        total_elapsed_time = (ua_date - transfer_date).days
    elif ua_date is not pd.NaT:
        total_elapsed_time = (ua_date - complaint_date).days
    else:
        total_elapsed_time = np.nan
    if total_elapsed_time == 0:
        total_elapsed_time = 1
    return total_elapsed_time


df = pd.DataFrame(df)

df['complaint_date'] = pd.to_datetime(df['complaint_date'])
df['ifp_submission_date'] = pd.to_datetime(df['ifp_submission_date'])
df['ifp_order_submission_date'] = pd.to_datetime(df['ifp_order_submission_date'])
df['partial_payment_date'] = pd.to_datetime(df['partial_payment_date'])
df['transfer_date'] = pd.to_datetime(df['transfer_date'])

df['IFP_Submission_Elapsed_Time'] = (df['ifp_submission_date'] - df['complaint_date']).dt.days
df['IFP_Submission_Elapsed_Time'] = df['IFP_Submission_Elapsed_Time'].astype('Int64')
df['IFP_Submission_Elapsed_Time'].fillna(0, inplace=True)

df['IFP_Order_Elapsed_Time'] = (df['ifp_order_submission_date'] - df['ifp_submission_date']).dt.days
df['IFP_Order_Elapsed_Time'] = df['IFP_Order_Elapsed_Time'].astype('Int64')
df['IFP_Order_Elapsed_Time'].fillna(0, inplace=True)

df['Payment_Elapsed_Time'] = (df['partial_payment_date'] - df['ifp_order_submission_date']).dt.days
df['Payment_Elapsed_Time'] = df['Payment_Elapsed_Time'].astype('Int64')
df['Payment_Elapsed_Time'].fillna(0, inplace=True)

df['Total_Elapsed_Time'] = df.apply(
    lambda x: _calculate_total_elapsed_time(x['complaint_date'], x['transfer_date'], x['ua_date'],
                                            x['dismissal_date_for_no_trust_fund_statement'], x['full_fee_paid'],
                                            x['partial_payment_date']), axis=1)

df_not_ua = df[df['Total_Elapsed_Time'].isnull()]
df_ua = df.loc[df['Total_Elapsed_Time'].notnull()]
df_ua['Total_Elapsed_Time'] = df_ua['Total_Elapsed_Time'].astype('Int64')


# df['Total_Elapsed_Time'].fillna(0, inplace=True)


df_ua_agg = df_ua[['caseid', 'case_type', 'case_number', 'Total_Elapsed_Time', 'IFP_Submission_Elapsed_Time',
                   'IFP_Order_Elapsed_Time', 'Payment_Elapsed_Time']]
# df_not_ua_agg = df_not_ua[['caseid', 'case_type', 'case_number', 'Total_Elapsed_Time', 'IFP_Submission_Elapsed_Time',
#                            'IFP_Order_Elapsed_Time', 'Payment_Elapsed_Time']]
with pd.ExcelWriter('data_files/green_belt_metrics.xlsx') as writer:
    df.to_excel(writer, sheet_name='Case Data Raw')
    df_ua.to_excel(writer, sheet_name='UA Case Data Raw')
    df_ua_agg.to_excel(writer, sheet_name='UA Case Data Times')
    # df_not_ua.to_excel(writer, sheet_name='Not UA Case Data Raw')
    # df_not_ua_agg.to_excel(writer, sheet_name='Not UA Case Data Times')
