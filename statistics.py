import pandas as pd
import numpy as np
import dill

with open('case_metrics.pkl', 'rb') as f:
    df = dill.load(f)

df = df[['Case ID', 'Case Number', 'Judge', 'Group', 'amended_complaint_count', 'case_reopen_count',
         'CMP To UA Elapsed', 'UA To LTP Elapsed', 'LTP to PPTCNF Elapsed', 'PPTCNF to SJ Elapsed',
         'SJ to FPTCNF Elapsed']]

pp = df.loc[df['Group'] == 'Prisoner Petitions']

caseid = 42990
target = df_entries.loc[df_entries['de_caseid'] == caseid]
orders = target.loc[(target['dp_dpseqno_ptr'].notnull()) & (target['dp_type'] == 'order')]
orders['dp_dpseqno_ptr']= orders.loc[orders['dp_dpseqno_ptr'].astype(int)]
replacement_text = orders.groupby('de_seqno')['dt_text'].apply(' '.join).reset_index()
orders = orders.drop(['dt_text'], axis=1, inplace=True)
orders.drop_duplicates(subset=['de_seqno'], keep='first', inplace=True)

orders = orders.merge(replacement_text, left_on='de_seqno', right_on='de_seqno', how='left')

order_motion = orders.merge(target, left_on='dp_dpseqno_ptr', right_on='dp_seqno', how='left')
order_motion = order_motion.drop(['dp_seqno_x','dp_deseqno_ptr_x', 'dp_dpseqno_ptr_x', 'dp_partno_x',
                   'de_caseid_y', 'de_seqno_y', 'dp_dpseqno_ptr_y', 'dp_deseqno_ptr_y',
                                  'dp_partno_y', 'dp_dispositive_y'], axis=1)

