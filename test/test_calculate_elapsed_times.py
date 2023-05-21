import datetime
from datetime import datetime

import pandas
import pandas as pd
import pytest

from green_belt_elapsed_times import _calculate_total_elapsed_time


@pytest.mark.parametrize('complaint_date, transfer_date, ua_date, dismissal_date_for_no_trust_fund_statement, '
                         'full_fee_paid, partial_payment_date, terminated_date, expected', [

                             (datetime(2020, 1, 2), pd.NaT, datetime(2020, 2, 28), pd.NaT, False,
                              datetime(2020, 2, 28), pd.NaT, 57)
                         ])
def test_calculate_elpased_time(complaint_date, transfer_date, ua_date,
                                dismissal_date_for_no_trust_fund_statement, full_fee_paid,
                                partial_payment_date, terminated_date, expected):
    num_days = _calculate_total_elapsed_time(complaint_date, transfer_date, ua_date,
                                             dismissal_date_for_no_trust_fund_statement, full_fee_paid,
                                             partial_payment_date, terminated_date)
    assert num_days == expected
