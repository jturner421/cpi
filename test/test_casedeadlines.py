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
                         [
                             (41669, '2018-04-20'),
                             (41648, '2018-04-19'),
                         ])
def test_for_complaint_date(docket_entries, caseid, expected):
    from ua_dates import _find_complaint
    target = docket_entries.loc[docket_entries['de_caseid'] == caseid]
    result = _find_complaint(target)
    assert result.complaint_date == expected


@pytest.mark.parametrize('caseid, complaint_date, expected',
                         [
                             (41669, '2018-04-20', None),
                         ])
def test_for_ifp_date(docket_entries, caseid, complaint_date, expected):
    from ua_dates import _get_ifp_date
    case_for_test = CaseDates(caseid=caseid, complaint_date=complaint_date)
    target = docket_entries.loc[docket_entries['de_caseid'] == case_for_test.caseid]
    result = _get_ifp_date(case_for_test, target)
    assert result.ifp_date == expected


@pytest.mark.parametrize('caseid, complaint_date, expected',
                         [
                             (41669, '2018-04-20', datetime(2018, 4, 30)),
                         ])
def test_identify_ua_dates_for_case(docket_entries, caseid, complaint_date, expected):
    from ua_dates import _get_ua_date
    case_for_test = CaseDates(caseid=caseid, complaint_date=complaint_date)
    target = docket_entries.loc[docket_entries['de_caseid'] == case_for_test.caseid]
    ua = target.loc[target['dp_sub_type'] == 'madv']
    result = _get_ua_date(case_for_test, ua)
    result.ua_date = datetime.strptime(result.ua_dates[0]['ua_date'], '%Y-%m-%d')
    assert result.ua_date == expected
