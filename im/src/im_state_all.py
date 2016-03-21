import datetime

from utils import *


def read_nc_data():
    # todo: read from csv written yesterday by this program
    nc_before_dates = pd.DatetimeIndex(['2016-03-17 23:50:00', '2016-03-17 23:50:00'])
    df_nc_before = pd.DataFrame(
        {TABLE_IM_NO_CONN_NC_STARTTIME: nc_before_dates, TABLE_IM_NO_CONN_ID: [False, True],
         TABLE_IM_NO_CONN_WTG_ID: ['wtg_001', 'wtg_002']})
    df_nc_before = df_nc_before.groupby([TABLE_IM_NO_CONN_WTG_ID]).apply(
        lambda x: x.sort_index(by=TABLE_IM_NO_CONN_NC_STARTTIME).iloc[-1])

    nc_dates = pd.DatetimeIndex(
        ['2016-03-18 08:05:00', '2016-03-18 08:40:00', '2016-03-18 09:40:00', '2016-03-18 10:30:00'])
    df_nc = pd.DataFrame({TABLE_IM_NO_CONN_NC_STARTTIME: nc_dates, TABLE_IM_NO_CONN_ID: [True, False, True, False],
                          TABLE_IM_NO_CONN_WTG_ID: ['wtg_001', 'wtg_002', 'wtg_001', 'wtg_002']}).sort_index(
        by=[TABLE_IM_NO_CONN_WTG_ID, TABLE_IM_NO_CONN_NC_STARTTIME])
    return df_nc_before, df_nc


def read_ss_data():
    # todo: read from csv written yesterday by this program
    ss_before_dates = pd.DatetimeIndex(['2016-03-17 23:30:00', '2016-03-17 23:30:00'])
    df_ss_before = pd.DataFrame({TABLE_IM_SS_STARTTIME: ss_before_dates, TABLE_IM_SS_ID: range(2),
                                 TABLE_IM_SS_WTG_ID: ['wtg_001', 'wtg_002']})
    df_ss_before = df_ss_before.groupby([TABLE_IM_SS_WTG_ID]).apply(
        lambda x: x.sort_index(by=TABLE_IM_SS_STARTTIME).iloc[-1])

    ss_today_dates = pd.DatetimeIndex(
        ['2016-03-18 08:30:00', '2016-03-18 09:30:00', '2016-03-18 09:45:00', '2016-03-18 11:30:00'])
    df_ss = pd.DataFrame(
        {TABLE_IM_SS_STARTTIME: ss_today_dates, TABLE_IM_SS_ID: range(4),
         TABLE_IM_SS_WTG_ID: ['wtg_001', 'wtg_002', 'wtg_001', 'wtg_002']}).sort_index(
        by=[TABLE_IM_SS_WTG_ID, TABLE_IM_SS_STARTTIME])

    return df_ss_before, df_ss


def read_data():
    df_nc_before, df_nc = read_nc_data()
    df_ss_before, df_ss = read_ss_data()
    return df_nc_before, df_nc, df_ss_before, df_ss


# @profile
def fill_in_data(last, all_records, time_column):
    add_first, add_last = True, True

    first_time = all_records.iloc[0][time_column].to_datetime()
    if first_time.hour == 0 and first_time.minute == 0 and first_time.second == 0:
        add_first = False
    last_time = all_records.iloc[-1][time_column].to_datetime()
    if last_time.hour == 23 and last_time.minute == 59 and last_time.second == 59:
        add_last = False

    if add_first:
        new_first = last.copy()
        new_first[time_column] = first_time.replace(hour=0, minute=0, second=0)
        all_records = pd.concat([pd.DataFrame([new_first]), all_records], ignore_index=True)

    if add_last:
        new_last = all_records.iloc[-1].copy()
        new_last[time_column] = last_time.replace(hour=23, minute=59, second=59)
        all_records = pd.concat([all_records, pd.DataFrame([new_last])], ignore_index=True)

    all_records = all_records.set_index(pd.DatetimeIndex(all_records[time_column]))

    all_seconds = [None] * (24 * 60 * 60)
    last_second = all_records.iloc[0]
    i = 0
    for s in pd.date_range(all_records.index[0], all_records.index[-1], freq='s'):
        if s in all_records.index:
            last_second = all_records.loc[s]
        r = last_second.copy()
        r[time_column] = s
        all_seconds[i] = r
        i += 1
    return pd.DataFrame(all_seconds)


def cal_im_turbine_state(turbine, nc_last, nc_all, ss_last, ss_all):
    nc_all = fill_in_data(nc_last, nc_all, TABLE_IM_NO_CONN_NC_STARTTIME)
    ss_all = fill_in_data(ss_last, ss_all, TABLE_IM_SS_STARTTIME)
    turbine_all = pd.merge(nc_all, ss_all, left_on=[TABLE_IM_NO_CONN_NC_STARTTIME, TABLE_IM_NO_CONN_WTG_ID],
                           right_on=[TABLE_IM_SS_STARTTIME, TABLE_IM_SS_WTG_ID], suffixes=['_NC', '_SS'])
    turbine_all.to_csv('.'.join([turbine, 'STATE_ALL', str(datetime.datetime.now().strftime('%Y-%m-%d'))]))


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
        '.'.join([IM_STATE_ALL_SS_TURBINE_LAST_RECORD, str(datetime.datetime.now().strftime('%Y-%m-%d'))]))


def cal_im_state_all():
    df_nc_before, df_nc, df_ss_before, df_ss = read_data()

    save_today_last(df_nc_before, df_nc, df_ss_before, df_ss)

    nc_dict = dict(list(df_nc.groupby([TABLE_IM_NO_CONN_WTG_ID])))
    ss_dict = dict(list(df_ss.groupby([TABLE_IM_SS_WTG_ID])))
    all_turbines = set(nc_dict.keys()) | set(ss_dict.keys())
    for t in all_turbines:
        cal_im_turbine_state(t, df_nc_before.loc[t], nc_dict.get(t), df_ss_before.loc[t], ss_dict.get(t))


if __name__ == '__main__':
    cal_im_state_all()
