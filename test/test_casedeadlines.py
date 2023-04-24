import datetime
from datetime import datetime
import pytest

from ua_dates import CaseDeadlines, CaseDates, _get_amended_complaints, get_docker_entries_for_case, \
    combine_docket_text_into_one_row


def _create_case(caseid, complaint_date, ifp_date=None):
    return CaseDates(caseid=caseid, complaint_date=complaint_date, ifp_date=ifp_date)


def test_field_access():
    cd = CaseDeadlines(caseid=12345,
                       pptcnf_date=datetime(2018, 4, 1),
                       dispositve_deadline=datetime(2019, 4, 1))

    assert cd.caseid == 12345
    assert cd.pptcnf_date == datetime(2018, 4, 1)
    assert cd.dispositve_deadline == datetime(2019, 4, 1)
    assert cd.limine_deadline is None


def test_docket_entries(docket_entries):
    assert docket_entries.size == 2562014


def test_docket_entries_for_a_case(docket_entries):
    caseid = 41669
    target = docket_entries.loc[docket_entries['de_caseid'] == caseid]
    assert target.shape[0] == 83


# @pytest.mark.parametrize('caseid, expected',
#                          [(41669, '2018-04-20'),
#                           (43516, '2018-07-11')])
# def test_for_complaint_date(docket_entries, caseid, expected):
#     from ua_dates import _find_complaint
#     target = _docket_entries_for_case(docket_entries, caseid)
#     # target = docket_entries.loc[docket_entries['de_caseid'] == caseid]
#     case = _find_complaint(target)
#     assert case.complaint_date == expected


# @pytest.mark.parametrize('caseid, complaint_date, expected',
#                          [(41669, '2018-04-20', 0),
#                           (43516, '2018-7-11', 1)])
# def test_for_amended_complaints(docket_entries, caseid, complaint_date, expected):
#     case = _create_case(caseid, complaint_date)
#     target = _docket_entries_for_case(docket_entries, caseid)
#     # target = docket_entries.loc[docket_entries['de_caseid'] == caseid]
#     case = _get_amended_complaints(case, target)
#     assert len(case.amended_complaints) == expected
#
#
# @pytest.mark.parametrize("caseid, complaint_date, expected", [(41669, '2018-04-20', None),
#                                                               (41648, '2018-04-19', '2018-04-19')
#                                                               ])
# def test_for_ifp_date(caseid, complaint_date, expected, docket_entries):
#     from ua_dates import _get_ifp_date
#     case = _create_case(caseid, complaint_date)
#     target = _docket_entries_for_case(docket_entries, caseid)
#     case = _get_ifp_date(case, target)
#     assert case.ifp_date == expected
#
#
# @pytest.mark.parametrize('caseid, complaint_date, expected',
#                          [
#                              (41669, '2018-04-20', 2),
#                              (43516, '2018-7-11', 2),
#                          ])
# def test_identify_ua_dates_for_case(docket_entries, caseid, complaint_date, expected):
#     from ua_dates import _get_ua_date
#     case = _create_case(caseid, complaint_date)
#     target = _docket_entries_for_case(docket_entries, caseid)
#     ua = target.loc[target['dp_sub_type'] == 'madv']
#     case = _get_ua_date(case, ua)
#     assert len(case.ua_dates) == expected
#
#
# @pytest.mark.parametrize('caseid, complaint_date, expected',
#                          [
#                              (41669, '2018-04-20', 1),
#                              (43516, '2018-7-11', 2),
#                          ])
# def test_leave_to_proceed_dates(docket_entries, caseid, complaint_date, expected):
#     from ua_dates import _get_leave_to_proceed
#     case = _create_case(caseid, complaint_date)
#     target = _docket_entries_for_case(docket_entries, caseid)
#     case = _get_leave_to_proceed(case, target)
#     assert len(case.order_to_leave_dates) == expected
#
#
# def test_fixture(create_case, get_case_data, docket_entries):
#     case = create_case(41669, '2018-04-20')
#     docket_entries_for_case = get_docker_entries_for_case(docket_entries, case.caseid)
#     case_data = get_case_data(case, docket_entries_for_case)
#     assert case.complaint_date == '2018-04-20'


@pytest.mark.parametrize('caseid, complaint_date, amended_complaint_date, ua_date, ltp_date', [
    (43978, '2019-06-17', '2022-03-04', '2022-03-04', '2022-05-19'),
    (40288, '2017-07-06', '2020-04-24', '2021-04-15', '2021-07-08'),
    (43802, '2019-05-16', '2023-01-24', '2023-03-3', None),
    (43516, '2019-03-20', '2022-02-23', '2022-02-23', '2022-03-01'),
    (42990, '2018-12-04', None, '2019-02-04', '2022-01-25'),
    (42348, '2018-08-13', '2022-04-25', '2022-04-25', None),
    (42972, '2018-12-03', '2019-03-04', '2019-03-04', '2019-03-12'),
    (41091, '2018-01-03', '2018-10-03', '2018-02-23', '2019-04-16'),
    (45034, '2019-12-16', '2022-09-29', '2022-09-29', None),
    (41669, '2018-04-20', None, '2018-07-27', '2018-08-20'),

])
def test_create_dict(create_case, get_case_data, docket_entries, caseid, complaint_date, amended_complaint_date,
                     ua_date, ltp_date):
    case = create_case(caseid, complaint_date)
    docket_entries_for_case = get_docker_entries_for_case(case.caseid, docket_entries)
    case_data = get_case_data(case, docket_entries_for_case)
    case_dict = case_data.dict()
    assert case_dict['complaint_date'] == datetime.strptime(complaint_date, '%Y-%m-%d')
    if amended_complaint_date is None:
        assert case_dict['amended_complaint_date'] == None
    else:
        assert case_dict['amended_complaint_date'] == datetime.strptime(amended_complaint_date, '%Y-%m-%d')
    assert case_dict['ua_date'] == datetime.strptime(ua_date, '%Y-%m-%d')
    if ltp_date is None:
        assert case_dict['ltp_date'] == None
    else:
        assert case_dict['ltp_date'] == datetime.strptime(ltp_date, '%Y-%m-%d')
    assert case_dict['caseid'] == caseid

# @pytest.mark.parametrize('caseid, complaint_date, ifp_date, length',
#                          [(41099, '2018-01-04', '2018-01-22', 1),
#                           (46578, '2020-09-14', '2020-09-14', 0),
#                           ])
# def test_get_early_dismissal_dates(docket_entries, caseid, complaint_date, ifp_date, length):
#     from ua_dates import _early_dismissal
#     case = _create_case(caseid=caseid, complaint_date=complaint_date,
#                         ifp_date=ifp_date)
#     target = _docket_entries_for_case(docket_entries, caseid)
#     case = _early_dismissal(case, target)
#     assert len(case.early_dismissal_dates) == length
#
#
# @pytest.mark.parametrize('amended_complaint_date, expected', [(datetime(2022, 2, 23), datetime(2022, 2, 23))])
# def test_get_ua_date(docket_entries_for_case, case_with_parameters, amended_complaint_date, expected):
#     if case_with_parameters.transfer_date:
#         case_with_parameters.complaint_date = datetime.strptime(case_with_parameters.transfer_date, '%Y-%m-%d')
#     case_with_parameters.amended_complaint_date = amended_complaint_date
#     case_with_parameters._calculate_ua_date()
#     assert case_with_parameters.ua_date == expected
#
#
# @pytest.mark.parametrize('amended_complaint_date, ua_date, expected', [
#     (datetime(2022, 2, 23), datetime(2022, 2, 23), datetime(2022, 3, 1)),
# ])
# def test_leave_to_proceed(docket_entries_for_case, case_with_parameters, amended_complaint_date, ua_date, expected):
#     if case_with_parameters.transfer_date:
#         case_with_parameters.complaint_date = datetime.strptime(case_with_parameters.transfer_date, '%Y-%m-%d')
#     case_with_parameters.amended_complaint_date = amended_complaint_date
#     case_with_parameters.ua_date = ua_date
#     case_with_parameters._calculate_ltp_date()
#     assert case_with_parameters.ltp_date == expected
