from utils import *


def read_data(turbine):
    date = execute_day_str
    turbine_cal_real = pd.read_csv(os.sep.join([OUTPUT_DIR, date, TABLE_IM_10M_CAL_REAL, turbine]),
                                   parse_dates=[STG_FACT_WTG_10M_DATATIME])
    turbine_cal_real = turbine_cal_real.set_index(pd.DatetimeIndex(turbine_cal_real[STG_FACT_WTG_10M_DATATIME]))
    turbine_state_all = pd.read_pickle(os.sep.join([OUTPUT_DIR, date, TABLE_IM_STATE_ALL, turbine]))
    turbine_state_all = turbine_state_all.set_index(pd.DatetimeIndex(turbine_state_all[STG_FACT_NO_CONN_NC_STARTTIME]))
    return turbine_cal_real, turbine_state_all


# @profile
def round_production(df):
    time_condition = df[STG_FACT_WTG_10M_DATATIME].apply(
        lambda x: x.minute in [9, 19, 29, 39, 49, 59] and x.second == 59)
    df[PRODUCTION_PER_SECOND][time_condition] = np.round(
        df[CAL_REAL_APPRODUCTION_10M] - 599 * df[PRODUCTION_PER_SECOND], decimals=5)
    df[THEORY_PRODUCTION_PER_SECOND][time_condition] = np.round(
        df[CAL_THEORY_APPRODUCTION_10M] - 599 * df[THEORY_PRODUCTION_PER_SECOND], decimals=5)
    df[ACTUAL_PRODUCTION_PER_SECOND][time_condition] = np.round(
        df[CAL_ACTIVE_APPRODUCTION_10M] - 599 * df[ACTUAL_PRODUCTION_PER_SECOND], decimals=5)


# @profile
def fill_in_data(turbine_real_all):
    turbine_real_all[PRODUCTION_PER_SECOND] = np.round(turbine_real_all[CAL_REAL_APPRODUCTION_10M] / (10 * 60),
                                                       decimals=5)
    turbine_real_all[THEORY_PRODUCTION_PER_SECOND] = np.round(turbine_real_all[CAL_THEORY_APPRODUCTION_10M] / (10 * 60),
                                                              decimals=5)
    turbine_real_all[ACTUAL_PRODUCTION_PER_SECOND] = np.round(turbine_real_all[CAL_ACTIVE_APPRODUCTION_10M] / (10 * 60),
                                                              decimals=5)
    turbine_real_all = turbine_real_all.asfreq('1s', method='pad')
    turbine_real_all[STG_FACT_WTG_10M_DATATIME] = turbine_real_all.index
    round_production(turbine_real_all)
    return turbine_real_all


# @profile
def write_data(turbine, turbine_base):
    out_dir = os.sep.join([OUTPUT_DIR, execute_day_str, IM_PRODUCTION_BASE])
    make_dirs(out_dir)
    turbine_base.to_pickle(os.sep.join([out_dir, str(turbine)]))


# @profile
def cal_turbine_production_base(turbine, real_all, state_all):
    real_all = fill_in_data(real_all)
    turbine_base = pd.merge(real_all, state_all, on=[STG_FACT_WTG_10M_WTG_ID], left_index=True,
                            right_index=True, how='outer')
    turbine_base[STG_FACT_WTG_10M_TEMOUTAVE] -= CONSTANT_TEMPERATURE
    write_data(turbine, turbine_base)
    del turbine_base
    del real_all
    del state_all


# @profile
def cal_production_base():
    turbines = read_all_turbines()
    for t in turbines:
        print t
        turbine_cal_real, turbine_state_all = read_data(t)
        cal_turbine_production_base(t, turbine_cal_real, turbine_state_all)


if __name__ == '__main__':
    cal_production_base()
