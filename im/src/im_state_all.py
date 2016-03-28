from utils import *


def read_data(table):
    if table == STG_FACT_NO_CONN:
        start_time_col, wtg_col = STG_FACT_NO_CONN_NC_STARTTIME, STG_FACT_NO_CONN_WTG_ID
        out_columns = [STG_FACT_NO_CONN_WTG_ID, STG_FACT_NO_CONN_NC_ID, STG_FACT_NO_CONN_NC_STARTTIME,
                       STG_FACT_NO_CONN_NC_STARTTIME_MSEC]
    elif table == STG_FACT_STANDARD_STATE:
        start_time_col, wtg_col = STG_FACT_STANDARD_STATE_SS_STARTTIME, STG_FACT_STANDARD_STATE_WTG_ID
        out_columns = [STG_FACT_STANDARD_STATE_WTG_ID, STG_FACT_STANDARD_STATE_SS_ID,
                       STG_FACT_STANDARD_STATE_SS_STARTTIME, STG_FACT_STANDARD_STATE_SS_STARTTIME_MSEC]
    elif table == STG_FACT_HEALTH_STATE:
        start_time_col, wtg_col = STG_FACT_HEALTH_STATE_SC_STARTTIME, STG_FACT_HEALTH_STATE_WTG_ID
        out_columns = [STG_FACT_HEALTH_STATE_WTG_ID, STG_FACT_HEALTH_STATE_SC_ID, STG_FACT_HEALTH_STATE_SC_STARTTIME,
                       STG_FACT_HEALTH_STATE_SC_STARTTIME_MSEC]

    df_before_dir = os.sep.join([INPUT_DIR, UTIL_YESTERDAY, table])
    df_dir = os.sep.join([INPUT_DIR, execute_day_str, table])

    make_dirs(df_before_dir)
    yesterday_files = []
    for f in os.listdir(df_before_dir):
        yesterday_files.append(
            pd.read_csv(os.sep.join([df_before_dir, f]), parse_dates=[start_time_col]))
    if len(yesterday_files) > 0:
        df_yesterday = pd.concat(yesterday_files)
        df_yesterday = df_yesterday[df_yesterday[start_time_col] < execute_day]
    else:
        df_yesterday = pd.DataFrame(None)

    df = pd.read_csv(df_dir, parse_dates=[start_time_col])
    if not df.empty:
        df_mapping = pd.read_csv(os.sep.join([INPUT_DIR, execute_day_str, DIM_ALIAS_ENVID_MAPPING]))
        df = pd.merge(df, df_mapping, left_on=wtg_col, right_on=DIM_ALIAS_ENVID_MAPPING_IDX)
        df[wtg_col] = df[DIM_ALIAS_ENVID_MAPPING_MDM_ID]

    df = pd.concat([df_yesterday, df])
    return df[out_columns]


def read_nc_data():
    return read_data(STG_FACT_NO_CONN)


def read_ss_data():
    return read_data(STG_FACT_STANDARD_STATE)


def read_hs_data():
    return read_data(STG_FACT_HEALTH_STATE)


def save_last_record(turbine, df, status_type):
    if status_type == 0:
        out_dir = STG_FACT_NO_CONN
    elif status_type == 1:
        out_dir = STG_FACT_STANDARD_STATE
    elif status_type == 2:
        out_dir = STG_FACT_HEALTH_STATE
    out_dir = os.sep.join([INPUT_DIR, UTIL_YESTERDAY, out_dir, turbine])
    df.to_pickle(out_dir)


# @profile
def fill_in_data(turbine, df, status_type):
    if status_type == 0:
        id_col, time_col, msec_col, status_col = STG_FACT_NO_CONN_WTG_ID, \
                                                 STG_FACT_NO_CONN_NC_STARTTIME, \
                                                 STG_FACT_NO_CONN_NC_STARTTIME_MSEC, \
                                                 STG_FACT_NO_CONN_NC_ID
    elif status_type == 1:
        id_col, time_col, msec_col, status_col = STG_FACT_STANDARD_STATE_WTG_ID, \
                                                 STG_FACT_STANDARD_STATE_SS_STARTTIME, \
                                                 STG_FACT_STANDARD_STATE_SS_STARTTIME_MSEC, \
                                                 STG_FACT_STANDARD_STATE_SS_ID
    elif status_type == 2:
        id_col, time_col, msec_col, status_col = STG_FACT_HEALTH_STATE_WTG_ID, \
                                                 STG_FACT_HEALTH_STATE_SC_STARTTIME, \
                                                 STG_FACT_HEALTH_STATE_SC_STARTTIME_MSEC, \
                                                 STG_FACT_HEALTH_STATE_SC_ID

    start = pd.DataFrame({status_col: np.nan}, index=pd.DatetimeIndex([execute_day_first_second]))
    # the last record of 23:59:59 need to be padded, so end time need to be today
    end = pd.DataFrame(None, index=pd.DatetimeIndex([today]))

    if df is None:
        df = pd.concat([start, end])
        df = df.asfreq('1s', method='pad')
        df[time_col] = df.index
        df[id_col] = turbine
        return df[(df.index >= execute_day) & (df.index < today)]

    # sort by start time column and msec col
    # filter multi records in one second, preserve the last one
    # set index
    df = df.sort_index(by=[time_col, msec_col])
    df = df.groupby([time_col]).apply(lambda x: (x.iloc[-1]))
    df = df.set_index(pd.DatetimeIndex(df[time_col]))
    del df[msec_col]
    save_last_record(turbine, df.iloc[-1:], status_type)

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
    out_columns = [STG_FACT_NO_CONN_WTG_ID, STG_FACT_NO_CONN_NC_STARTTIME, STG_FACT_NO_CONN_NC_ID,
                   STG_FACT_STANDARD_STATE_SS_ID, STG_FACT_HEALTH_STATE_SC_ID]
    state_all[out_columns].to_pickle(os.sep.join([out_dir, turbine]))


# @profile
def cal_im_turbine_state(t, t_nc, t_ss, t_hs):
    print t
    t_nc = fill_in_data(t, t_nc, 0)
    t_ss = fill_in_data(t, t_ss, 1)
    t_hs = fill_in_data(t, t_hs, 2)

    state_all = pd.merge(t_nc, t_ss, on=[STG_FACT_NO_CONN_WTG_ID],
                         left_index=True, right_index=True, how='outer')
    state_all = pd.merge(state_all, t_hs, on=[STG_FACT_NO_CONN_WTG_ID],
                         left_index=True, right_index=True, how='outer')

    state_all[STG_FACT_NO_CONN_NC_STARTTIME] = state_all.index
    state_all[STG_FACT_STANDARD_STATE_SS_STARTTIME] = state_all.index
    state_all[STG_FACT_HEALTH_STATE_SC_STARTTIME] = state_all.index
    write_turbine_state(t, state_all)


# @profile
def cal_im_state_all():
    df_nc, df_ss, df_hs = read_nc_data(), read_ss_data(), read_hs_data()

    nc_dict = dict(list(df_nc.groupby([STG_FACT_NO_CONN_WTG_ID])))
    ss_dict = dict(list(df_ss.groupby([STG_FACT_STANDARD_STATE_WTG_ID])))
    hs_dict = dict(list(df_hs.groupby([STG_FACT_HEALTH_STATE_WTG_ID])))

    all_turbines = set(nc_dict.keys()) | set(ss_dict.keys()) | set(hs_dict.keys())
    all_10m_turbines = set(read_all_turbines())
    all_turbines = all_turbines & all_10m_turbines
    for t in all_turbines:
        print t
        cal_im_turbine_state(t, nc_dict.get(t), ss_dict.get(t), hs_dict.get(t))


if __name__ == '__main__':
    cal_im_state_all()
