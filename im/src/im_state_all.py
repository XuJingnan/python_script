import datetime
import os

from utils import *


def sort_get_last_record(group):
    return pd.DataFrame([group.sort().iloc[-1]])


def read_nc_data():
    # todo: read from csv written yesterday by this program
    nc_before_dates = pd.DatetimeIndex(['2016-03-17 23:50:00', '2016-03-17 23:50:00', '2016-03-17 23:55:00'])
    df_nc_before = pd.DataFrame(
        {TABLE_IM_NO_CONN_ID: [True, True, True],
         TABLE_IM_NO_CONN_WTG_ID: ['430000001', '430000002', '430000001']}, index=nc_before_dates)
    groups = df_nc_before.groupby(by=[TABLE_IM_NO_CONN_WTG_ID])
    df_nc_before = group_process(groups, sort_get_last_record)

    nc_dates = pd.DatetimeIndex(
        ['2016-03-18 08:05:00', '2016-03-18 08:40:00', '2016-03-18 09:40:00', '2016-03-18 10:30:00'])
    df_nc = pd.DataFrame({TABLE_IM_NO_CONN_ID: [False, False, True, True],
                          TABLE_IM_NO_CONN_WTG_ID: ['430000001', '430000002', '430000001', '430000002']},
                         index=nc_dates)
    return df_nc_before, df_nc


def read_ss_data():
    # todo: read from csv written yesterday by this program
    ss_before_dates = pd.DatetimeIndex(['2016-03-17 23:30:00', '2016-03-17 23:30:00'])
    df_ss_before = pd.DataFrame({TABLE_IM_SS_ID: range(2),
                                 TABLE_IM_SS_WTG_ID: ['430000001', '430000002']}, index=ss_before_dates)
    groups = df_ss_before.groupby(by=[TABLE_IM_SS_WTG_ID])
    df_ss_before = group_process(groups, sort_get_last_record)

    ss_today_dates = pd.DatetimeIndex(
        ['2016-03-18 08:30:00', '2016-03-18 09:30:00', '2016-03-18 09:45:00', '2016-03-18 11:30:00'])
    df_ss = pd.DataFrame({TABLE_IM_SS_ID: range(4), TABLE_IM_SS_WTG_ID: [
        '430000001', '430000002', '430000001', '430000002']}, index=ss_today_dates)
    return df_ss_before, df_ss


def read_data():
    df_nc_before, df_nc = read_nc_data()
    df_ss_before, df_ss = read_ss_data()
    return df_nc_before, df_nc, df_ss_before, df_ss


# @profile
def fill_in_data(last, all_records):
    add_first, add_last = True, True
    first_time = all_records.index[0].to_datetime()
    if first_time.hour == 0 and first_time.minute == 0 and first_time.second == 0:
        add_first = False
    last_time = all_records.index[-1].to_datetime()
    if last_time.hour == 23 and last_time.minute == 59 and last_time.second == 59:
        add_last = False

    if add_first:
        new_first = last.copy()
        new_first = new_first.set_index(pd.DatetimeIndex([first_time.replace(hour=0, minute=0, second=0)]))
        all_records = pd.concat([new_first, all_records])

    if add_last:
        new_last = all_records.iloc[-1:].copy()
        new_last = new_last.set_index(pd.DatetimeIndex([last_time.replace(hour=23, minute=59, second=59)]))
        all_records = pd.concat([all_records, new_last])
    all_records = all_records.asfreq('1s', method='pad')
    return all_records


# @profile
def write_turbine_state(turbine, turbine_state_all):
    date = str(datetime.datetime.now().strftime('%Y-%m-%d'))
    out_dir = os.sep.join([OUTPUT_DIR, date, TURBINE_STATE_ALL])
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    turbine_state_all.to_csv(os.sep.join([out_dir, turbine]))


# @profile
def cal_im_turbine_state(turbine, nc_last, nc_all, ss_last, ss_all):
    nc_all = fill_in_data(nc_last, nc_all)
    ss_all = fill_in_data(ss_last, ss_all)
    turbine_all = pd.merge(nc_all, ss_all, left_on=[TABLE_IM_NO_CONN_WTG_ID],
                           right_on=[TABLE_IM_SS_WTG_ID], left_index=True, right_index=True,
                           suffixes=['_NC', '_SS'])
    write_turbine_state(turbine, turbine_all)


def save_today_last(df_nc_before, df_nc, df_ss_before, df_ss):
    df_nc_today = df_nc.groupby([TABLE_IM_NO_CONN_WTG_ID]).apply(
        lambda x: x.sort_index(by=TABLE_IM_NO_CONN_NC_STARTTIME).iloc[-1])
    df_nc_total = pd.concat([df_nc_before, df_nc_today]).groupby([TABLE_IM_NO_CONN_WTG_ID]).apply(
        lambda x: x.sort_index(by=TABLE_IM_NO_CONN_NC_STARTTIME).iloc[-1])
    df_nc_total.to_csv(
        '.'.join([IM_STATE_ALL_NC_TURBINE_LAST_RECORD, str(datetime.datetime.now().strftime('%Y-%m-%d'))]))

    df_ss_today = df_ss.groupby([TABLE_IM_SS_WTG_ID]).apply(
        lambda x: x.sort_index(by=TABLE_IM_SS_STARTTIME).iloc[-1])
    df_ss_total = pd.concat([df_ss_before, df_ss_today]).groupby([TABLE_IM_SS_WTG_ID]).apply(
        lambda x: x.sort_index(by=TABLE_IM_SS_STARTTIME).iloc[-1])
    df_ss_total.to_csv(
        '.'.join([IM_STATE_ALL_SS_TURBINE_LAST_RECORD, str(datetime.datetime.now().strftime('%Y-%m-%d'))]), index=False)


def cal_im_state_all():
    df_nc_before, df_nc, df_ss_before, df_ss = read_data()

    # save_today_last(df_nc_before, df_nc, df_ss_before, df_ss)

    nc_dict = dict(list(df_nc.groupby([TABLE_IM_NO_CONN_WTG_ID])))
    nc_before_dict = dict(list(df_nc_before.groupby([TABLE_IM_NO_CONN_WTG_ID])))
    ss_dict = dict(list(df_ss.groupby([TABLE_IM_SS_WTG_ID])))
    ss_before_dict = dict(list(df_ss_before.groupby([TABLE_IM_SS_WTG_ID])))
    all_turbines = set(nc_dict.keys()) | set(ss_dict.keys())
    for t in all_turbines:
        cal_im_turbine_state(t, nc_before_dict.get(t), nc_dict.get(t), ss_before_dict.get(t), ss_dict.get(t))


if __name__ == '__main__':
    cal_im_state_all()
