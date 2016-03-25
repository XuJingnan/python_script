from utils import *


def read_nc_data():
    df_nc_before_dir = os.sep.join([INPUT_DIR, UTIL_YESTERDAY, TABLE_IM_NO_CONN])
    make_dirs(df_nc_before_dir)
    yesterday_nc_files = []
    for f in os.listdir(df_nc_before_dir):
        yesterday_nc_files.append(
            pd.read_csv(os.sep.join([df_nc_before_dir, f]), parse_dates=[TABLE_IM_NO_CONN_NC_STARTTIME]))
    if len(yesterday_nc_files) > 0:
        df_yesterday_nc = pd.concat(yesterday_nc_files)
        df_yesterday_nc = df_yesterday_nc[df_yesterday_nc[TABLE_IM_NO_CONN_NC_STARTTIME] < execute_day]
    else:
        df_yesterday_nc = pd.DataFrame(None)

    df_nc = pd.read_csv(os.sep.join([INPUT_DIR, execute_day_str, TABLE_IM_NO_CONN]),
                        parse_dates=[TABLE_IM_NO_CONN_NC_STARTTIME])
    df_mapping = pd.read_csv(os.sep.join([INPUT_DIR, execute_day_str, TABLE_MDM_ALIAS_ENVID_MAPPING]))
    df_nc = pd.merge(df_nc, df_mapping, left_on=TABLE_IM_NO_CONN_WTG_ID, right_on=TABLE_MDM_ALIAS_ENVID_MAPPING_IDX)
    df_nc[TABLE_IM_NO_CONN_WTG_ID] = df_nc[TABLE_MDM_ALIAS_ENVID_MAPPING_MDM_ID]
    df_nc = pd.concat([df_yesterday_nc, df_nc])
    df_nc = df_nc.set_index(pd.DatetimeIndex(df_nc[TABLE_IM_NO_CONN_NC_STARTTIME]))
    nc_columns = [TABLE_IM_NO_CONN_WTG_ID, TABLE_IM_NO_CONN_NC_STARTTIME, TABLE_IM_NO_CONN_ID]
    return df_nc[nc_columns]


def read_ss_data():
    df_ss_before_dir = os.sep.join([INPUT_DIR, UTIL_YESTERDAY, TABLE_IM_SS])
    make_dirs(df_ss_before_dir)
    yesterday_ss_files = []
    for f in os.listdir(df_ss_before_dir):
        yesterday_ss_files.append(pd.read_csv(os.sep.join([df_ss_before_dir, f]), parse_dates=[TABLE_IM_SS_STARTTIME]))
    if len(yesterday_ss_files) > 0:
        df_yesterday_ss = pd.concat(yesterday_ss_files)
        df_yesterday_ss = df_yesterday_ss[df_yesterday_ss[TABLE_IM_SS_STARTTIME] < execute_day]
    else:
        df_yesterday_ss = pd.DataFrame(None)

    df_ss = pd.read_csv(os.sep.join([INPUT_DIR, execute_day_str, TABLE_IM_SS]), parse_dates=[TABLE_IM_SS_STARTTIME])
    df_mapping = pd.read_csv(os.sep.join([INPUT_DIR, execute_day_str, TABLE_MDM_ALIAS_ENVID_MAPPING]))
    df_ss = pd.merge(df_ss, df_mapping, left_on=TABLE_IM_SS_WTG_ID, right_on=TABLE_MDM_ALIAS_ENVID_MAPPING_IDX)
    df_ss[TABLE_IM_SS_WTG_ID] = df_ss[TABLE_MDM_ALIAS_ENVID_MAPPING_MDM_ID]
    df_ss = pd.concat([df_yesterday_ss, df_ss])
    df_ss = df_ss.set_index(pd.DatetimeIndex(df_ss[TABLE_IM_SS_STARTTIME]))
    ss_columns = [TABLE_IM_SS_WTG_ID, TABLE_IM_SS_STARTTIME, TABLE_IM_SS_ID]
    return df_ss[ss_columns]


# todo need to check raw data
def filter_conflict_time(df, time_col):
    groups = df.groupby(by=[time_col])
    df = groups.apply(lambda x: x.iloc[-1])
    return df


# @profile
def fill_in_data(turbine, df, status_type):
    if status_type == 0:
        id_col, time_col, status_col = TABLE_IM_NO_CONN_WTG_ID, TABLE_IM_NO_CONN_NC_STARTTIME, TABLE_IM_NO_CONN_ID
    else:
        id_col, time_col, status_col = TABLE_IM_SS_WTG_ID, TABLE_IM_SS_STARTTIME, TABLE_IM_SS_ID

    start = pd.DataFrame({status_col: np.nan}, index=pd.DatetimeIndex([execute_day_first_second]))
    # the last record of 23:59:59 need to be padded, so end time need to be today
    end = pd.DataFrame(None, index=pd.DatetimeIndex([today]))

    if df is None:
        df = pd.concat([start, end])
        df = df.asfreq('1s', method='pad')
        df[time_col] = df.index
        df[id_col] = turbine
        return df[(df.index >= execute_day) & (df.index < today)]

    df = df.sort_index(by=[time_col])
    save_last_record(turbine, df.iloc[-1:], status_type)
    df = filter_conflict_time(df, time_col)

    add_first, add_last = True, True
    first_time = df.index[0].to_datetime()
    last_time = df.index[-1].to_datetime()
    if is_execute_day_before_more(first_time):
        start = pd.DataFrame({status_col: df.iloc[0][status_col]},
                             index=pd.DatetimeIndex([execute_day_first_second]))
        df = df.iloc[1:]
        add_first = True
    if is_execute_day_first_second(first_time):
        add_first = False
    if is_execute_day_last_second(last_time):
        add_last = False
    if add_first:
        df = pd.concat([start, df])
    if add_last:
        df = pd.concat([df, end])
    df = df.asfreq('1s', method='pad')
    df[id_col] = turbine
    df[time_col] = df.index
    return df[(df.index >= execute_day) & (df.index < today)]


# @profile
def write_turbine_state(turbine, state_all):
    out_dir = os.sep.join([OUTPUT_DIR, execute_day_str, TABLE_IM_STATE_ALL])
    make_dirs(out_dir)
    state_all.to_pickle(os.sep.join([out_dir, turbine]))


# @profile
def cal_im_turbine_state(t, t_nc, t_ss):
    print t
    t_nc = fill_in_data(t, t_nc, 0)
    t_ss = fill_in_data(t, t_ss, 1)
    state_all = pd.merge(t_nc, t_ss, on=[TABLE_IM_NO_CONN_WTG_ID], left_index=True, right_index=True, how='outer')
    state_all[TABLE_IM_NO_CONN_NC_STARTTIME] = state_all.index
    state_all[TABLE_IM_SS_STARTTIME] = state_all.index
    write_turbine_state(t, state_all)


def save_last_record(turbine, df, status_type):
    out_dir = TABLE_IM_NO_CONN if status_type == 0 else TABLE_IM_SS
    out_dir = os.sep.join([INPUT_DIR, UTIL_YESTERDAY, out_dir, turbine])
    df.to_csv(out_dir, index=False)


# @profile
def cal_im_state_all():
    df_nc, df_ss = read_nc_data(), read_ss_data()

    nc_dict = dict(list(df_nc.groupby([TABLE_IM_NO_CONN_WTG_ID])))
    ss_dict = dict(list(df_ss.groupby([TABLE_IM_SS_WTG_ID])))

    all_turbines = set(nc_dict.keys()) | set(ss_dict.keys())
    all_10m_turbines = set(read_all_turbines())
    all_turbines = all_turbines & all_10m_turbines
    for t in all_turbines:
        cal_im_turbine_state(t, nc_dict.get(t), ss_dict.get(t))


if __name__ == '__main__':
    cal_im_state_all()
