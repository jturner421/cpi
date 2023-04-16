import datetime
from datetime import datetime

import pytest

from ua_dates import CaseDeadlines, CaseDates


def test_field_access():
    cd = CaseDeadlines(caseid=12345,
                       pptcnf_date=datetime(2018, 4, 1),
                       dispositve_deadline=datetime(2019, 4, 1))

    assert cd.caseid == 12345
    assert cd.pptcnf_date == datetime(2018, 4, 1)
    assert cd.dispositve_deadline == datetime(2019, 4, 1)
    assert cd.limine_deadline is None


def test_docket_entries(docket_entries):
    assert docket_entries.size == 1637667


def test_docket_entries_for_a_case(docket_entries):
    caseid = 41669
    target = docket_entries.loc[docket_entries['de_caseid'] == caseid]
    assert target.shape[0] == 83


@pytest.mark.parametrize('caseid, expected',
                         [(41669, '2018-04-20')])
def test_for_complaint_date(docket_entries, caseid, expected):
    from ua_dates import _find_complaint
    target = _docket_entries_for_case(docket_entries, caseid)
    # target = docket_entries.loc[docket_entries['de_caseid'] == caseid]
    case = _find_complaint(target)
    assert case.complaint_date == expected


@pytest.mark.parametrize("caseid, complaint_date, expected", [(41669, '2018-04-20', None),
                                                              (41648, '2018-04-19', '2018-04-19')
                                                              ])
def test_for_ifp_date(caseid, complaint_date, expected, docket_entries):
    from ua_dates import _get_ifp_date
    case = _create_case(caseid, complaint_date)
    target = _docket_entries_for_case(docket_entries, caseid)
    case = _get_ifp_date(case, target)
    assert case.ifp_date == expected


def _create_case(caseid, complaint_date):
    return CaseDates(caseid=caseid, complaint_date=complaint_date)


def _docket_entries_for_case(docket_entries, caseid):
    docket_entries_for_case = docket_entries.loc[docket_entries['de_caseid'] == caseid]
    return docket_entries_for_case

# @pytest.mark.parametrize('caseid, complaint_date, expected',
#                          [
#                              (41669, '2018-04-20', 3),
#                              (41648, '2018-04-19', 1),
#                          ])
# def test_identify_ua_dates_for_case(docket_entries, caseid, complaint_date, expected):
#     from ua_dates import _get_ua_date
#     case_for_test = CaseDates(caseid=caseid, complaint_date=complaint_date)
#     target = docket_entries.loc[docket_entries['de_caseid'] == case_for_test.caseid]
#     ua = target.loc[target['dp_sub_type'] == 'madv']
#     case_for_test = _get_ua_date(case_for_test, ua)
#     assert len(case_for_test.ua_dates) == expected
#
# def get_ua_date_for_case(docket_entries_for_case):
#     pass
