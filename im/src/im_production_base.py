from utils import *


def read_data(turbine):
    date = str(datetime.datetime.now().strftime('%Y-%m-%d'))
    turbine_cal_real = pd.read_csv(os.sep.join([OUTPUT_DIR, date, TABLE_IM_10M_CAL_REAL, turbine]))
    turbine_cal_real = turbine_cal_real.set_index(pd.DatetimeIndex(turbine_cal_real[TABLE_TBL_POINTVALUE_10M_DATATIME]))
    turbine_state_all = pd.read_csv(os.sep.join([OUTPUT_DIR, date, TABLE_IM_STATE_ALL, turbine]))
    turbine_state_all = turbine_state_all.set_index(pd.DatetimeIndex(turbine_state_all[TABLE_IM_NO_CONN_NC_STARTTIME]))
    return turbine_cal_real, turbine_state_all


def fill_in_data(turbine_real_all):
    turbine_real_all[TABLE_IM_PRODUCTION_BASE_PRODUCTION_PER_SECOND] = turbine_real_all[CAL_REAL_APPRODUCTION_10M] / (
        10 * 60)
    turbine_real_all = turbine_real_all.asfreq('1s', method='pad')
    turbine_real_all[TABLE_TBL_POINTVALUE_10M_DATATIME] = turbine_real_all.index
    return turbine_real_all


def write_data(turbine, turbine_base):
    date = str(datetime.datetime.now().strftime('%Y-%m-%d'))
    out_dir = os.sep.join([OUTPUT_DIR, date, TABLE_IM_PRODUCTION_BASE])
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    turbine_base.to_csv(os.sep.join([out_dir, str(turbine)]))


def cal_turbine_production_base(turbine, real_all, state_all):
    real_all = fill_in_data(real_all)
    turbine_base = pd.merge(real_all, state_all, on=[TABLE_TBL_POINTVALUE_10M_WTG_ID], left_index=True,
                            right_index=True, how='outer')
    turbine_base[TABLE_TBL_POINTVALUE_10M_TEMOUTAVE] -= CONSTANT_TEMPERATURE
    write_data(turbine, turbine_base)
    del turbine_base
    del real_all
    del state_all


def cal_production_base():
    turbines = read_all_turbines()
    for t in turbines:
        turbine_cal_real, turbine_state_all = read_data(t)
        cal_turbine_production_base(t, turbine_cal_real, turbine_state_all)


if __name__ == '__main__':
    cal_production_base()
