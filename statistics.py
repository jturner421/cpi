import pandas as pd
import dill

with open('case_metrics.pkl', 'rb') as f:
    df = dill.load(f)

df = df[['Case ID', 'Case Number', 'Judge', 'Group', 'amended_complaint_count', 'case_reopen_count',
         'CMP To UA Elapsed','UA To LTP Elapsed','LTP to PPTCNF Elapsed','PPTCNF to SJ Elapsed','SJ to FPTCNF Elapsed']]

pp = df.loc[df['Group'] == 'Prisoner Petitions']