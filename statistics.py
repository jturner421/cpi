import pandas as pd
import numpy as np
import dill
import openpyxl

with open('data_files/case_metrics.pkl', 'rb') as f:
    df = dill.load(f)

df = df.drop(['dispositive_deadline', 'limine_deadline', 'fptcnf_date', 'trial_date'], axis=1)
closed = df.loc[df["Total Time Elapsed"].notnull()]
closed_aggregate = closed[['Case Number', 'Judge', 'Group', 'CMP To UA Elapsed', 'UA To LTP Elapsed',
                            'CMP To LTP Elapsed', 'LTP to PPTCNF Elapsed', 'Discovery Elapsed',
                            'SJ to Disposition Elapsed', 'LTP to Termination Elapsed', 'Total Time Elapsed']]
pending = df.loc[df["Total Time Elapsed"].isnull()]
pending_aggregate = pending[['Case Number', 'Judge', 'Group', 'CMP To UA Elapsed', 'UA To LTP Elapsed',
                             'CMP To LTP Elapsed', 'LTP to PPTCNF Elapsed', 'Discovery Elapsed',
                             'SJ to Disposition Elapsed', 'LTP to Termination Elapsed', 'Total Time Elapsed']]


with pd.ExcelWriter('data_files/case_metrics.xlsx') as writer:
    closed.to_excel(writer, sheet_name='Closed Cases Raw')
    closed_aggregate.to_excel(writer, sheet_name='Closed Cases Aggregate')
    pending.to_excel(writer, sheet_name='Pending Cases Raw')
    pending_aggregate.to_excel(writer, sheet_name='Pending Cases Aggregate')
