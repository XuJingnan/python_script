from utils import *


def sort_get_last_record(wtg_id, group):
    return pd.DataFrame([group.sort().iloc[-1]])


def read_nc_data():
    df_nc_before_path = os.sep.join([INPUT_DIR, UTIL_YESTERDAY, TABLE_IM_NO_CONN, TABLE_IM_NO_CONN])
    if os.path.exists(df_nc_before_path):
        df_nc_before = pd.read_csv(df_nc_before_path)
        df_nc_before = df_nc_before.set_index(pd.DatetimeIndex(df_nc_before[TABLE_IM_NO_CONN_NC_STARTTIME]))
    else:
        df_nc_before = pd.DataFrame(None)

    df_nc = pd.read_csv(os.sep.join([INPUT_DIR, get_date(), TABLE_IM_NO_CONN]))
    df_nc = df_nc.set_index(pd.DatetimeIndex(df_nc[TABLE_IM_NO_CONN_NC_STARTTIME]))
    return df_nc_before, df_nc


def read_ss_data():
    df_ss_before_path = os.sep.join([INPUT_DIR, UTIL_YESTERDAY, TABLE_IM_SS, TABLE_IM_SS])
    if os.path.exists(df_ss_before_path):
        df_ss_before = pd.read_csv(df_ss_before_path)
        df_ss_before = df_ss_before.set_index(pd.DatetimeIndex(df_ss_before[TABLE_IM_SS_STARTTIME]))
    else:
        df_ss_before = pd.DataFrame(None)

    df_ss = pd.read_csv(os.sep.join([INPUT_DIR, get_date(), TABLE_IM_SS]))
    df_ss = df_ss.set_index(pd.DatetimeIndex(df_ss[TABLE_IM_SS_STARTTIME]))
    return df_ss_before, df_ss


def read_data():
    df_nc_before, df_nc = read_nc_data()
    df_ss_before, df_ss = read_ss_data()
    return df_nc_before, df_nc, df_ss_before, df_ss


# @profile
def fill_in_data(last, all_records, time_column):
    all_records = all_records.sort()
    add_first, add_last = True, True
    first_time = all_records.index[0].to_datetime()
    if first_time.hour == 0 and first_time.minute == 0 and first_time.second == 0:
        add_first = False
    last_time = all_records.index[-1].to_datetime()
    if last_time.hour == 23 and last_time.minute == 59 and last_time.second == 59:
        add_last = False

    if add_first and last is not None and first_time > last.index[0].to_datetime():
        new_first = last.copy()
        new_first = new_first.set_index(pd.DatetimeIndex([first_time.replace(hour=0, minute=0, second=0)]))
        new_first[time_column] = new_first.index
        all_records = pd.concat([new_first, all_records])
    if add_last:
        new_last = all_records.iloc[-1:].copy()
        new_last = new_last.set_index(pd.DatetimeIndex([last_time.replace(hour=23, minute=59, second=59)]))
        new_last[time_column] = new_last.index
        all_records = pd.concat([all_records, new_last])
    all_records = all_records.asfreq('1s', method='pad')
    return all_records


# @profile
def write_turbine_state(turbine, turbine_state_all):
    date = get_date()
    out_dir = os.sep.join([OUTPUT_DIR, date, TABLE_IM_STATE_ALL])
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    turbine_state_all.to_csv(os.sep.join([out_dir, str(turbine)]), index=False)


# @profile
def cal_im_turbine_state(turbine, nc_last, nc_all, ss_last, ss_all):
    nc_all = fill_in_data(nc_last, nc_all, TABLE_IM_NO_CONN_NC_STARTTIME)
    ss_all = fill_in_data(ss_last, ss_all, TABLE_IM_SS_STARTTIME)
    turbine_all = pd.merge(nc_all, ss_all, on=[TABLE_IM_NO_CONN_WTG_ID], left_index=True, right_index=True,
                           suffixes=['_NC', '_SS'])
    turbine_all[TABLE_IM_NO_CONN_NC_STARTTIME] = turbine_all.index
    turbine_all[TABLE_IM_SS_STARTTIME] = turbine_all.index
    write_turbine_state(turbine, turbine_all)


def save_today_last(df_nc_before, df_nc, df_ss_before, df_ss):
    df_nc_total = pd.concat([df_nc_before, df_nc])
    nc_groups = df_nc_total.groupby([TABLE_IM_NO_CONN_WTG_ID])
    df_nc_total = group_process(nc_groups, sort_get_last_record, True)
    df_nc_total.to_csv(os.sep.join([INPUT_DIR, UTIL_YESTERDAY, TABLE_IM_NO_CONN, TABLE_IM_NO_CONN]), index=False)

    df_ss_total = pd.concat([df_ss_before, df_ss])
    ss_groups = df_ss_total.groupby([TABLE_IM_SS_WTG_ID])
    df_ss_total = group_process(ss_groups, sort_get_last_record, True)
    df_ss_total.to_csv(os.sep.join([INPUT_DIR, UTIL_YESTERDAY, TABLE_IM_SS, TABLE_IM_SS]), index=False)


def cal_im_state_all():
    df_nc_before, df_nc, df_ss_before, df_ss = read_data()

    nc_dict = dict(list(df_nc.groupby([TABLE_IM_NO_CONN_WTG_ID])))
    if not df_nc_before.empty:
        nc_before_dict = dict(list(df_nc_before.groupby([TABLE_IM_NO_CONN_WTG_ID])))
    else:
        nc_before_dict = {}
    ss_dict = dict(list(df_ss.groupby([TABLE_IM_SS_WTG_ID])))
    if not df_ss_before.empty:
        ss_before_dict = dict(list(df_ss_before.groupby([TABLE_IM_SS_WTG_ID])))
    else:
        ss_before_dict = {}

    all_turbines = set(nc_dict.keys()) | set(ss_dict.keys())
    for t in all_turbines:
        cal_im_turbine_state(t, nc_before_dict.get(t), nc_dict.get(t), ss_before_dict.get(t), ss_dict.get(t))

    save_today_last(df_nc_before, df_nc, df_ss_before, df_ss)


if __name__ == '__main__':
    cal_im_state_all()
