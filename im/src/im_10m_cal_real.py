from utils import *


def read_data(turbine):
    date = execute_day_str
    df_10m_clean = pd.read_csv(os.sep.join([OUTPUT_DIR, date, TABLE_IM_10M_CLEAN, turbine]),
                               parse_dates=[TABLE_TBL_POINTVALUE_10M_DATATIME])
    df_turbine = pd.read_csv(os.sep.join([INPUT_DIR, date, IM_DIM_WIND_WTG]))
    df_site = pd.read_csv(os.sep.join([INPUT_DIR, date, IM_DIM_WIND_SITE]))
    df_ntf = pd.read_csv(os.sep.join([INPUT_DIR, date, TABLE_MDM_NTF_DATA]))
    df_power_curve = pd.read_csv(os.sep.join([INPUT_DIR, date, TABLE_MDM_POWERCURVE_DATA]))
    return df_10m_clean, df_turbine, df_site, df_ntf, df_power_curve


def write_data(turbine, df):
    print 'start write ', turbine
    # only get execute day records
    df = df[df[TABLE_TBL_POINTVALUE_10M_DATATIME] >= execute_day_first_second]
    date = execute_day_str
    out_dir = os.sep.join([OUTPUT_DIR, date, TABLE_IM_10M_CAL_REAL])
    make_dirs(out_dir)
    df.to_csv(os.sep.join([out_dir, turbine]), index=False)


def merge_all_table(df_10m_clean, df_turbine, df_site):
    df_clean_turbine_merge = pd.merge(df_10m_clean, df_turbine, left_on=TABLE_TBL_POINTVALUE_10M_WTG_ID,
                                      right_on=IM_DIM_WIND_WTG_WTG_ID)
    df_merge_all = pd.merge(df_clean_turbine_merge, df_site, left_on=IM_DIM_WIND_WTG_SITE_ID,
                            right_on=IM_DIM_WIND_SITE_SITE_ID)
    return df_merge_all


# @profile
def func_ntf_wind_speed_apply(df, out_columns):
    df = df.reset_index()
    df = df.sort_index(by=[TABLE_MDM_NTF_DATA_ROTORSPEED], ascending=[True])

    first_row = df.iloc[0]
    rotor_speed_filter = clean_flag_check_is_normal(first_row[TABLE_IM_10M_CLEAN_CLEAN_FLAG],
                                                    TABLE_TBL_POINTVALUE_10M_ROTORSPDAVE)
    wind_speed_filter = clean_flag_check_is_normal(first_row[TABLE_IM_10M_CLEAN_CLEAN_FLAG],
                                                   TABLE_TBL_POINTVALUE_10M_WINDSPEEDAVE)
    ntf_filter = (df[TABLE_TBL_POINTVALUE_10M_ROTORSPDAVE] >= df[TABLE_MDM_NTF_DATA_ROTORSPEED])
    ntf_index = ntf_filter[ntf_filter == True].last_valid_index()

    if rotor_speed_filter & wind_speed_filter & (ntf_index is not None):
        s = df.iloc[ntf_index].copy()
        s[CAL_NTF_WIND_SPEED] = s[TABLE_MDM_NTF_DATA_A] * (s[TABLE_TBL_POINTVALUE_10M_WINDSPEEDAVE] ** 2) + \
                                s[TABLE_MDM_NTF_DATA_B] * s[TABLE_TBL_POINTVALUE_10M_WINDSPEEDAVE] + \
                                s[TABLE_MDM_NTF_DATA_C]
    else:
        s = first_row.copy()
        s[CAL_NTF_WIND_SPEED] = np.nan
    return s[out_columns]


# @profile
# calculate ntf wind speed, three condition will be nan:
# 1. rotor not normal
# 2. wind speed not normal
# 3. rotor too small and ntf table not find
def cal_ntf(df_merge_all, df_ntf):
    in_columns = [TABLE_TBL_POINTVALUE_10M_WTG_ID, TABLE_TBL_POINTVALUE_10M_DATATIME,
                  TABLE_TBL_POINTVALUE_10M_TEMOUTAVE, TABLE_TBL_POINTVALUE_10M_WINDSPEEDAVE,
                  TABLE_TBL_POINTVALUE_10M_ROTORSPDAVE, TABLE_IM_10M_CLEAN_CLEAN_FLAG, IM_DIM_WIND_WTG_SCADA_NTF_ID,
                  IM_DIM_WIND_SITE_AIR_DENSITY, IM_DIM_WIND_WTG_ALTITUDE, IM_DIM_WIND_WTG_HUB_HEIGHT,
                  IM_DIM_WIND_WTG_CONTRACT_PC_ID, IM_DIM_WIND_WTG_ACTUAL_PC_ID]
    df_merge_all = df_merge_all[in_columns]
    groups = pd.merge(df_merge_all, df_ntf, left_on=IM_DIM_WIND_WTG_SCADA_NTF_ID,
                      right_on=TABLE_MDM_NTF_DATA_PARENTID).groupby([TABLE_TBL_POINTVALUE_10M_DATATIME])
    out_columns = [TABLE_TBL_POINTVALUE_10M_WTG_ID, TABLE_TBL_POINTVALUE_10M_DATATIME,
                   TABLE_TBL_POINTVALUE_10M_TEMOUTAVE, TABLE_IM_10M_CLEAN_CLEAN_FLAG,
                   CAL_NTF_WIND_SPEED, IM_DIM_WIND_SITE_AIR_DENSITY, IM_DIM_WIND_WTG_ALTITUDE,
                   IM_DIM_WIND_WTG_HUB_HEIGHT, IM_DIM_WIND_WTG_CONTRACT_PC_ID, IM_DIM_WIND_WTG_ACTUAL_PC_ID]
    return groups.apply(func_ntf_wind_speed_apply, out_columns)


def cal_10m_ave_air_pressure(altitude, hub_height, temperature):
    return 101.325 * np.exp(-1.0 * (altitude + hub_height) * 9.8 / (287.05 * (273.15 + temperature)))


def cal_10m_ave_vapour_pressure(temp):
    return 0.0000205 * np.exp(0.0613846 * temp)


def cal_10m_air_density(row):
    temp, altitude, hub_height = row[TABLE_TBL_POINTVALUE_10M_TEMOUTAVE], row[IM_DIM_WIND_WTG_ALTITUDE], row[
        IM_DIM_WIND_WTG_HUB_HEIGHT]
    return 1.0 / temp * (cal_10m_ave_air_pressure(altitude, hub_height, temp) * 1000 / CONSTANT_GAS_DRY_AIR -
                         CONSTANT_RELATIVE_HUMIDITY * cal_10m_ave_vapour_pressure(temp) * (
                             1 / CONSTANT_GAS_DRY_AIR - 1 / CONSTANT_GAS_WATER_VAPOUR))


# @profile
# calculate ntf site wind speed and ntf standard wind speed, some condition will be nan:
# 1. ntf wind speed is nan
def cal_ntf_site_standard(df):
    in_columns = [TABLE_TBL_POINTVALUE_10M_WTG_ID, TABLE_TBL_POINTVALUE_10M_DATATIME,
                  TABLE_TBL_POINTVALUE_10M_TEMOUTAVE, TABLE_IM_10M_CLEAN_CLEAN_FLAG,
                  CAL_NTF_WIND_SPEED, IM_DIM_WIND_SITE_AIR_DENSITY, IM_DIM_WIND_WTG_ALTITUDE,
                  IM_DIM_WIND_WTG_HUB_HEIGHT, IM_DIM_WIND_WTG_CONTRACT_PC_ID, IM_DIM_WIND_WTG_ACTUAL_PC_ID]
    df = df[in_columns]
    temperature_condition = df[TABLE_IM_10M_CLEAN_CLEAN_FLAG].apply(
        lambda x: clean_flag_check_is_normal(x, TABLE_TBL_POINTVALUE_10M_TEMOUTAVE))
    df[CAL_AIR_DENSITY] = np.nan
    df[CAL_AIR_DENSITY][temperature_condition] = df.apply(cal_10m_air_density, axis=1)
    df[CAL_NTF_WIND_SPEED_SITE] = df[CAL_NTF_WIND_SPEED] * (
        (df[CAL_AIR_DENSITY] / df[IM_DIM_WIND_SITE_AIR_DENSITY]) ** (1.0 / 3))
    df[CAL_NTF_WIND_SPEED_STANDARD] = df[CAL_NTF_WIND_SPEED] * (
        (df[CAL_AIR_DENSITY] / CONSTANT_STANDARD_AIR_DENSITY) ** (1.0 / 3))
    out_columns = [TABLE_TBL_POINTVALUE_10M_WTG_ID, TABLE_TBL_POINTVALUE_10M_DATATIME, CAL_AIR_DENSITY,
                   CAL_NTF_WIND_SPEED, CAL_NTF_WIND_SPEED_SITE, CAL_NTF_WIND_SPEED_STANDARD,
                   IM_DIM_WIND_WTG_CONTRACT_PC_ID, IM_DIM_WIND_WTG_ACTUAL_PC_ID]
    return df[out_columns]


# @profile
# calculate ntf / ntf site / ntf standard
def cal_ntfs(df_merge_all, df_ntf):
    df_merge_all = cal_ntf(df_merge_all, df_ntf)
    df_all_ntfs = cal_ntf_site_standard(df_merge_all)
    return df_all_ntfs


# calculate real production
# input: clean + ntf data for each turbine
# production error condition:
# 1. decrease, tag 1
# 2. out of max range, tag 2
# 3. nan, tag 4
# 4. previous production nan, tag 8
# 5. ntf wind speed clean, tag 16
# 6. ntf site wind speed clean, tag 32
# 7. ntf standard wind speed clean, tag 64
# 8. no temperature
def cal_wtg_real_production(df_wtg):
    in_columns = [TABLE_TBL_POINTVALUE_10M_WTG_ID, TABLE_TBL_POINTVALUE_10M_DATATIME,
                  TABLE_TBL_POINTVALUE_10M_TEMOUTAVE, TABLE_TBL_POINTVALUE_10M_WINDDIRECTIONAVE,
                  TABLE_TBL_POINTVALUE_10M_NACELLEPOSITIONAVE, TABLE_TBL_POINTVALUE_10M_BLADEPITCHAVE,
                  TABLE_TBL_POINTVALUE_10M_WINDSPEEDAVE, TABLE_TBL_POINTVALUE_10M_WINDSPEEDSTD,
                  TABLE_TBL_POINTVALUE_10M_ROTORSPDAVE, TABLE_TBL_POINTVALUE_10M_GENSPDAVE,
                  TABLE_TBL_POINTVALUE_10M_TORQUESETPOINTAVE, TABLE_TBL_POINTVALUE_10M_TORQUEAVE,
                  TABLE_TBL_POINTVALUE_10M_ACTIVEPWAVE, TABLE_TBL_POINTVALUE_10M_PCURVESTSAVE,
                  TABLE_TBL_POINTVALUE_10M_APPRODUCTION, TABLE_TBL_POINTVALUE_10M_RPPRODUCTION,
                  TABLE_TBL_POINTVALUE_10M_APCONSUMED, TABLE_TBL_POINTVALUE_10M_RPCONSUMED,
                  TABLE_IM_10M_CLEAN_CLEAN_FLAG, CAL_AIR_DENSITY, CAL_NTF_WIND_SPEED, CAL_NTF_WIND_SPEED_SITE,
                  CAL_NTF_WIND_SPEED_STANDARD, IM_DIM_WIND_WTG_CONTRACT_PC_ID, IM_DIM_WIND_WTG_ACTUAL_PC_ID]
    df_wtg = df_wtg[in_columns]

    df_wtg = df_wtg.sort_index(by=[TABLE_TBL_POINTVALUE_10M_DATATIME])
    df_wtg[CAL_REAL_APPRODUCTION_10M] = df_wtg[TABLE_TBL_POINTVALUE_10M_APPRODUCTION] - df_wtg[
        TABLE_TBL_POINTVALUE_10M_APPRODUCTION].shift(1)

    # process decrease, out of max range, nan, previous nan condition
    production_normal_condition = df_wtg[TABLE_IM_10M_CLEAN_CLEAN_FLAG].apply(
        lambda x: clean_flag_check_is_normal(x, TABLE_TBL_POINTVALUE_10M_APPRODUCTION))
    df_wtg[CAL_REAL_APPRODUCTION_10M][~ production_normal_condition] = np.nan
    df_wtg[CAL_REAL_APPRODUCTION_10M_ERRORS] = df_wtg[TABLE_IM_10M_CLEAN_CLEAN_FLAG].apply(
        lambda x: clean_flag_get(x, TABLE_TBL_POINTVALUE_10M_APPRODUCTION))

    # filter no ntf wind speed
    condition = df_wtg[CAL_NTF_WIND_SPEED].isnull()
    df_wtg[CAL_REAL_APPRODUCTION_10M][condition] = np.nan
    df_wtg[CAL_REAL_APPRODUCTION_10M_ERRORS][condition] = df_wtg[CAL_REAL_APPRODUCTION_10M_ERRORS].apply(
        lambda x: production_error_set(x, PRODUCTION_ERROR_NO_NTF_WIND_SPEED))

    # filter no ntf site wind speed
    condition = df_wtg[CAL_NTF_WIND_SPEED_SITE].isnull()
    df_wtg[CAL_REAL_APPRODUCTION_10M][condition] = np.nan
    df_wtg[CAL_REAL_APPRODUCTION_10M_ERRORS][condition] = df_wtg[CAL_REAL_APPRODUCTION_10M_ERRORS].apply(
        lambda x: production_error_set(x, PRODUCTION_ERROR_NO_NTF_SITE_WIND_SPEED))

    # filter no ntf standard wind speed
    condition = df_wtg[CAL_NTF_WIND_SPEED_STANDARD].isnull()
    df_wtg[CAL_REAL_APPRODUCTION_10M][condition] = np.nan
    df_wtg[CAL_REAL_APPRODUCTION_10M_ERRORS][condition] = df_wtg[CAL_REAL_APPRODUCTION_10M_ERRORS].apply(
        lambda x: production_error_set(x, PRODUCTION_ERROR_NO_NTF_STANDARD_WIND_SPEED))

    # filter no temperature
    condition = df_wtg[TABLE_IM_10M_CLEAN_CLEAN_FLAG].apply(
        lambda x: clean_flag_check_is_normal(x, TABLE_TBL_POINTVALUE_10M_TEMOUTAVE))
    df_wtg[CAL_REAL_APPRODUCTION_10M][~ condition] = np.nan
    df_wtg[CAL_REAL_APPRODUCTION_10M_ERRORS][~ condition] = df_wtg[CAL_REAL_APPRODUCTION_10M_ERRORS].apply(
        lambda x: production_error_set(x, PRODUCTION_ERROR_NO_TEMP_OUT))

    # cal rp production, ap consumption, rp consumption
    df_wtg[CAL_REAL_RPPRODUCTION_10M] = df_wtg[TABLE_TBL_POINTVALUE_10M_RPPRODUCTION] - df_wtg[
        TABLE_TBL_POINTVALUE_10M_RPPRODUCTION].shift(1)
    df_wtg[CAL_REAL_APCONSUMED_10M] = df_wtg[TABLE_TBL_POINTVALUE_10M_APCONSUMED] - df_wtg[
        TABLE_TBL_POINTVALUE_10M_APCONSUMED].shift(1)
    df_wtg[CAL_REAL_RPCONSUMED_10M] = df_wtg[TABLE_TBL_POINTVALUE_10M_RPCONSUMED] - df_wtg[
        TABLE_TBL_POINTVALUE_10M_RPCONSUMED].shift(1)

    out_columns = [TABLE_TBL_POINTVALUE_10M_WTG_ID, TABLE_TBL_POINTVALUE_10M_DATATIME,
                   TABLE_TBL_POINTVALUE_10M_TEMOUTAVE, TABLE_TBL_POINTVALUE_10M_WINDDIRECTIONAVE,
                   TABLE_TBL_POINTVALUE_10M_NACELLEPOSITIONAVE, TABLE_TBL_POINTVALUE_10M_BLADEPITCHAVE,
                   TABLE_TBL_POINTVALUE_10M_WINDSPEEDAVE, TABLE_TBL_POINTVALUE_10M_WINDSPEEDSTD,
                   TABLE_TBL_POINTVALUE_10M_ROTORSPDAVE, TABLE_TBL_POINTVALUE_10M_GENSPDAVE,
                   TABLE_TBL_POINTVALUE_10M_TORQUESETPOINTAVE, TABLE_TBL_POINTVALUE_10M_TORQUEAVE,
                   TABLE_TBL_POINTVALUE_10M_ACTIVEPWAVE, TABLE_TBL_POINTVALUE_10M_PCURVESTSAVE,
                   TABLE_TBL_POINTVALUE_10M_APPRODUCTION, TABLE_TBL_POINTVALUE_10M_RPPRODUCTION,
                   TABLE_TBL_POINTVALUE_10M_APCONSUMED, TABLE_TBL_POINTVALUE_10M_RPCONSUMED,
                   TABLE_IM_10M_CLEAN_CLEAN_FLAG, CAL_AIR_DENSITY, CAL_NTF_WIND_SPEED, CAL_NTF_WIND_SPEED_SITE,
                   CAL_NTF_WIND_SPEED_STANDARD, IM_DIM_WIND_WTG_CONTRACT_PC_ID, IM_DIM_WIND_WTG_ACTUAL_PC_ID,
                   CAL_REAL_APPRODUCTION_10M, CAL_REAL_APPRODUCTION_10M_ERRORS, CAL_REAL_RPPRODUCTION_10M,
                   CAL_REAL_APCONSUMED_10M, CAL_REAL_RPCONSUMED_10M]
    return df_wtg[out_columns]


def cal_predict_power(v0, p0, v1, p1, v2):
    return (p1 - p0) / (v1 - v0) * (v2 - v0) + p0


# @profile
# calculate theory production and active production, some conditions will be nan
# 1. ntf standard wind speed is nan
# pc_type = 0: theory production
# pc_type = 1: actual production
def cal_predict_production(groups, pc_type):
    in_columns = [TABLE_TBL_POINTVALUE_10M_WTG_ID, TABLE_TBL_POINTVALUE_10M_DATATIME, CAL_NTF_WIND_SPEED_STANDARD,
                  TABLE_MDM_POWERCURVE_DATA_WINDSPEEDRANK, TABLE_MDM_POWERCURVE_DATA_POWER]
    # groups = groups[in_columns]
    predict_production_name = CAL_THEORY_APPRODUCTION_10M if pc_type == 0 else CAL_ACTIVE_APPRODUCTION_10M
    first_row = groups.iloc[0].copy()
    first_row[predict_production_name] = np.nan
    out_columns = [TABLE_TBL_POINTVALUE_10M_WTG_ID, TABLE_TBL_POINTVALUE_10M_DATATIME, predict_production_name]

    ntf_standard_normal_condition = groups[CAL_NTF_WIND_SPEED_STANDARD].notnull()
    if groups[ntf_standard_normal_condition].empty:
        return first_row[out_columns]
    sorted_group = groups.sort_index(by=[TABLE_MDM_POWERCURVE_DATA_WINDSPEEDRANK], ascending=[True])
    match = sorted_group[
        sorted_group[CAL_NTF_WIND_SPEED_STANDARD] >= sorted_group[TABLE_MDM_POWERCURVE_DATA_WINDSPEEDRANK]]
    if match.empty:
        return first_row[out_columns]
    before_row = match.iloc[-1]
    match = sorted_group[
        sorted_group[CAL_NTF_WIND_SPEED_STANDARD] < sorted_group[TABLE_MDM_POWERCURVE_DATA_WINDSPEEDRANK]]
    if match.empty:
        return first_row[out_columns]
    after_row = match.iloc[0]
    first_row[predict_production_name] = cal_predict_power(before_row[TABLE_MDM_POWERCURVE_DATA_WINDSPEEDRANK],
                                                           before_row[TABLE_MDM_POWERCURVE_DATA_POWER],
                                                           after_row[TABLE_MDM_POWERCURVE_DATA_WINDSPEEDRANK],
                                                           after_row[TABLE_MDM_POWERCURVE_DATA_POWER],
                                                           first_row[CAL_NTF_WIND_SPEED_STANDARD]) / 6
    return first_row[out_columns]


# @profile
def im_10m_turbine_cal_real(turbine):
    df_10m_clean, df_turbine, df_site, df_ntf, df_power_curve = read_data(turbine)
    df_merge_all = merge_all_table(df_10m_clean, df_turbine, df_site)
    df_ntf_all = cal_ntfs(df_merge_all, df_ntf)
    df_clean_ntf_merge = pd.merge(df_10m_clean, df_ntf_all,
                                  on=[TABLE_TBL_POINTVALUE_10M_WTG_ID, TABLE_TBL_POINTVALUE_10M_DATATIME], how='left')
    df_cal_real = cal_wtg_real_production(df_clean_ntf_merge)

    # calculate contract production
    groups = pd.merge(df_cal_real, df_power_curve, left_on=IM_DIM_WIND_WTG_CONTRACT_PC_ID,
                      right_on=TABLE_MDM_POWERCURVE_DATA_PARENTID).groupby(by=[TABLE_TBL_POINTVALUE_10M_DATATIME])
    df_contracts = groups.apply(cal_predict_production, 0)
    # calculate actual production
    groups = pd.merge(df_cal_real, df_power_curve, left_on=IM_DIM_WIND_WTG_ACTUAL_PC_ID,
                      right_on=TABLE_MDM_POWERCURVE_DATA_PARENTID).groupby(by=[TABLE_TBL_POINTVALUE_10M_DATATIME])
    df_actuals = groups.apply(cal_predict_production, 1)
    df_cal_all = pd.merge(df_cal_real, df_contracts,
                          on=[TABLE_TBL_POINTVALUE_10M_WTG_ID, TABLE_TBL_POINTVALUE_10M_DATATIME], how='left')
    df_cal_all = pd.merge(df_cal_all, df_actuals,
                          on=[TABLE_TBL_POINTVALUE_10M_WTG_ID, TABLE_TBL_POINTVALUE_10M_DATATIME], how='left')
    write_data(turbine, df_cal_all)


def im_10m_cal_real():
    turbines = read_all_turbines()
    for t in turbines:
        print 'start process', t
        im_10m_turbine_cal_real(t)


if __name__ == '__main__':
    im_10m_cal_real()
