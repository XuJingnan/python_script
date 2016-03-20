import datetime
import numpy as np

from utils import *


def read_data():
    # todo no_conn and ss yesterday last record
    time_suffix = '.' + str(datetime.datetime.now().strftime('%Y-%m-%d'))
    df_10m_clean = pd.read_csv(OUTPUT_DIR + TABLE_IM_10M_CLEAN + time_suffix,
                               parse_dates=[TABLE_TBL_POINTVALUE_10M_DATATIME])
    df_turbine = pd.read_csv(INPUT_DIR + IM_DIM_WIND_WTG + time_suffix)
    df_site = pd.read_csv(INPUT_DIR + IM_DIM_WIND_SITE + time_suffix)
    df_ntf = pd.read_csv(INPUT_DIR + TABLE_MDM_NTF_DATA + time_suffix)
    df_power_curve = pd.read_csv(INPUT_DIR + TABLE_IM_POWER_CURVE + time_suffix)
    return df_10m_clean, df_turbine, df_site, df_ntf, df_power_curve


def write_data(df):
    df.to_csv(OUTPUT_DIR + TABLE_IM_10M_CAL_REAL + '.' + str(datetime.datetime.now().strftime('%Y-%m-%d')))


def ntf_filter(df_clean_turbine_merge):
    rotor_speed_filter = df_clean_turbine_merge[TABLE_IM_10M_CLEAN_CLEAN_FLAG].apply(
        lambda x: clean_flag_check_is_normal(x, TABLE_TBL_POINTVALUE_10M_ROTORSPDAVE))
    wind_speed_filter = df_clean_turbine_merge[TABLE_IM_10M_CLEAN_CLEAN_FLAG].apply(
        lambda x: clean_flag_check_is_normal(x, TABLE_TBL_POINTVALUE_10M_WINDSPEEDAVE))
    return df_clean_turbine_merge[rotor_speed_filter & wind_speed_filter]


def func_ntf_wind_speed_apply(df_wtg_ntf_group):
    r = df_wtg_ntf_group.sort_index(by=[TABLE_MDM_NTF_DATA_ROTORSPEED], ascending=[True])[
        df_wtg_ntf_group[TABLE_TBL_POINTVALUE_10M_ROTORSPDAVE] > df_wtg_ntf_group[TABLE_MDM_NTF_DATA_ROTORSPEED]]
    if r.empty:
        nan_result = pd.Series(
            df_wtg_ntf_group.iloc[0][[TABLE_TBL_POINTVALUE_10M_WTG_ID, TABLE_TBL_POINTVALUE_10M_DATATIME, np.nan]],
            index=[TABLE_TBL_POINTVALUE_10M_WTG_ID, TABLE_TBL_POINTVALUE_10M_DATATIME, CAL_NTF_WIND_SPEED])
        return pd.DataFrame([nan_result])
    r = r.iloc[-1]
    r[CAL_NTF_WIND_SPEED] = r[TABLE_MDM_NTF_DATA_A] * (r[TABLE_TBL_POINTVALUE_10M_WINDSPEEDAVE] ** 2) + \
                            r[TABLE_MDM_NTF_DATA_B] * r[TABLE_TBL_POINTVALUE_10M_WINDSPEEDAVE] + \
                            r[TABLE_MDM_NTF_DATA_C]
    return pd.DataFrame([r[[TABLE_TBL_POINTVALUE_10M_WTG_ID, TABLE_TBL_POINTVALUE_10M_DATATIME, CAL_NTF_WIND_SPEED]]])


# calculate ntf wind speed, three condition will be nan:
# 1. rotor not normal
# 2. wind speed not normal
# 3. rotor too small and ntf table not find
def cal_ntf(df_10m_clean, df_turbine, df_ntf):
    df_clean_turbine_merge = pd.merge(df_10m_clean, df_turbine, left_on=TABLE_TBL_POINTVALUE_10M_WTG_ID,
                                      right_on=IM_DIM_WIND_WTG_WTG_ID, suffixes=['_clean', '_turbine'])
    df_clean_turbine_merge = ntf_filter(df_clean_turbine_merge)
    groups = pd.merge(df_clean_turbine_merge, df_ntf, left_on=IM_DIM_WIND_WTG_SCADANTF,
                      right_on=TABLE_MDM_NTF_DATA_PARENTID).groupby(
        [TABLE_TBL_POINTVALUE_10M_WTG_ID, TABLE_TBL_POINTVALUE_10M_DATATIME])
    res = group_process(groups, func_ntf_wind_speed_apply)
    return res[res[CAL_NTF_WIND_SPEED].notnull()]


def cal_10m_ave_air_pressure(altitude, hub_height, temperature):
    return 101.325 * np.exp(-1.0 * (altitude + hub_height) * 9.8 / (287.05 * (273.15 + temperature)))


def cal_10m_ave_vapour_pressure(temp):
    return 0.0000205 * np.exp(0.0613846 * temp)


def cal_10m_air_density(row):
    temp, altitude, hub_height = row[TABLE_TBL_POINTVALUE_10M_TEMOUTAVE], row[IM_DIM_WIND_WTG_ALTITUDE], row[
        IM_DIM_WIND_WTG_HUB_HEIGHT]
    return 1.0 / temp * (cal_10m_ave_air_pressure(altitude, hub_height, temp) / CONSTANT_GAS_DRY_AIR -
                         CONSTANT_RELATIVE_HUMIDITY * cal_10m_ave_vapour_pressure(temp) * (
                             1 / CONSTANT_GAS_DRY_AIR - 1 / CONSTANT_GAS_WATER_VAPOUR))


def ntf_site_standard_filter(df_clean_turbine_merge):
    temperature_filter = df_clean_turbine_merge[TABLE_IM_10M_CLEAN_CLEAN_FLAG].apply(
        lambda x: clean_flag_check_is_normal(x, TABLE_TBL_POINTVALUE_10M_TEMOUTAVE))
    return df_clean_turbine_merge[temperature_filter]


# WTG_ID, DATATIME, NTF_WIND_SPEED, TEMP, ALTITUDE, HUB_HEIGHT, AIR_DENSITY
def ntf_site_standard_merge_all_table(df_10m_clean, df_turbine, df_ntf_wind_speed, df_site):
    df_clean_turbine_merge = pd.merge(df_10m_clean, df_turbine, left_on=TABLE_TBL_POINTVALUE_10M_WTG_ID,
                                      right_on=IM_DIM_WIND_WTG_WTG_ID, suffixes=['_clean', '_turbine'])
    df_clean_turbine_merge = ntf_site_standard_filter(df_clean_turbine_merge)[
        [TABLE_TBL_POINTVALUE_10M_WTG_ID, TABLE_TBL_POINTVALUE_10M_DATATIME, TABLE_TBL_POINTVALUE_10M_TEMOUTAVE,
         IM_DIM_WIND_WTG_ALTITUDE, IM_DIM_WIND_WTG_HUB_HEIGHT, IM_DIM_WIND_WTG_SITE_ID]]
    df_ntf_clean_turbine_merge = pd.merge(df_ntf_wind_speed, df_clean_turbine_merge)
    df_site = df_site[[IM_DIM_WIND_SITE_SITE_ID, IM_DIM_WIND_SITE_AIRDENSITY]]
    df_all_merge = pd.merge(df_ntf_clean_turbine_merge, df_site, left_on=IM_DIM_WIND_WTG_SITE_ID,
                            right_on=IM_DIM_WIND_SITE_SITE_ID)
    return df_all_merge


# calculate ntf site wind speed and ntf standard wind speed, three condition will be nan:
# 1. temperature not normal
# todo temperature influence the result, especially temp = 0.000017, the 'cal air density' is 19999.8013243251
# todo furthermore, cal ntf site and standard wind speed will be influenced
# todo if temperature is negative, the ntf site and ntf standard can not be computed
# 2. ntf site wind speed or ntf standard wind speed > 100
# 3. ntf site wind speed or ntf standard wind speed is nan
def cal_ntf_site_standard(df_ntf_wind_speed, df_10m_clean, df_turbine, df_site):
    df_all_merge = ntf_site_standard_merge_all_table(df_10m_clean, df_turbine, df_ntf_wind_speed, df_site)
    cal_air_density = 'cal_air_density'
    df_all_merge[cal_air_density] = df_all_merge.apply(cal_10m_air_density, axis=1)
    df_all_merge[CAL_NTF_WIND_SPEED_SITE] = df_all_merge[CAL_NTF_WIND_SPEED] * (
        (df_all_merge[cal_air_density] / df_all_merge[IM_DIM_WIND_SITE_AIRDENSITY]) ** (1.0 / 3))
    df_all_merge[CAL_NTF_WIND_SPEED_STANDARD] = df_all_merge[CAL_NTF_WIND_SPEED] * (
        (df_all_merge[cal_air_density] / CONSTANT_STANDARD_AIR_DENSITY) ** (1.0 / 3))
    scope_filter = (df_all_merge[CAL_NTF_WIND_SPEED_SITE] < 100) & (df_all_merge[CAL_NTF_WIND_SPEED_STANDARD] < 100)
    nan_filter = df_all_merge[CAL_NTF_WIND_SPEED_SITE].notnull() & df_all_merge[CAL_NTF_WIND_SPEED_STANDARD].notnull()
    return df_all_merge[scope_filter & nan_filter]


def cal_ntfs(df_10m_clean, df_turbine, df_ntf, df_site):
    df_ntf_wind_speed = cal_ntf(df_10m_clean, df_turbine, df_ntf)
    df_all_merge = cal_ntf_site_standard(df_ntf_wind_speed, df_10m_clean, df_turbine, df_site)
    return df_all_merge[[TABLE_TBL_POINTVALUE_10M_WTG_ID, TABLE_TBL_POINTVALUE_10M_DATATIME, CAL_NTF_WIND_SPEED,
                         CAL_NTF_WIND_SPEED_SITE, CAL_NTF_WIND_SPEED_STANDARD]]


def cal_predict_power(v0, p0, v1, p1, v2):
    return (p1 - p0) / (v1 - v0) * (v2 - v0) + p0


def predict_production_filter(wtg_hour_groups):
    condition = wtg_hour_groups[CAL_NTF_WIND_SPEED_STANDARD].notnull()
    return wtg_hour_groups[condition]


# calculate theory production and active production, some conditions will be nan
# 1. ntf standard wind speed is nan
def cal_predict_production(wtg_hour_groups):
    first_row = wtg_hour_groups.iloc[0]
    wtg_hour_groups = predict_production_filter(wtg_hour_groups)
    sorted_group = wtg_hour_groups.sort_index(by=[TABLE_IM_POWER_CURVE_WIND_SPEED_RANK], ascending=[True])
    if sorted_group.empty:
        s = pd.Series([first_row[TABLE_TBL_POINTVALUE_10M_WTG_ID], first_row[TABLE_TBL_POINTVALUE_10M_DATATIME], np.nan,
                       np.nan], index=[TABLE_TBL_POINTVALUE_10M_WTG_ID, TABLE_TBL_POINTVALUE_10M_DATATIME,
                                       CAL_THEORY_APPRODUCTION_10M, CAL_ACTIVE_APPRODUCTION_10M])
        return pd.DataFrame([s])
    before_row = sorted_group[sorted_group[CAL_NTF_WIND_SPEED_STANDARD]
                              > sorted_group[TABLE_IM_POWER_CURVE_WIND_SPEED_RANK]].iloc[-1]
    after_row = sorted_group[sorted_group[CAL_NTF_WIND_SPEED_STANDARD]
                             < sorted_group[TABLE_IM_POWER_CURVE_WIND_SPEED_RANK]].iloc[0]
    thoery_production = cal_predict_power(before_row[TABLE_IM_POWER_CURVE_WIND_SPEED_RANK],
                                          before_row[TABLE_IM_POWER_CURVE_THOERY_POWER],
                                          after_row[TABLE_IM_POWER_CURVE_WIND_SPEED_RANK],
                                          after_row[TABLE_IM_POWER_CURVE_THOERY_POWER],
                                          first_row[CAL_NTF_WIND_SPEED_STANDARD]) / 6
    active_production = cal_predict_power(before_row[TABLE_IM_POWER_CURVE_WIND_SPEED_RANK],
                                          before_row[TABLE_IM_POWER_CURVE_ACTIVE_POWER],
                                          after_row[TABLE_IM_POWER_CURVE_WIND_SPEED_RANK],
                                          after_row[TABLE_IM_POWER_CURVE_ACTIVE_POWER],
                                          first_row[CAL_NTF_WIND_SPEED_STANDARD]) / 6
    return pd.DataFrame([pd.Series(
        [first_row[TABLE_TBL_POINTVALUE_10M_WTG_ID], first_row[TABLE_TBL_POINTVALUE_10M_DATATIME],
         thoery_production, active_production],
        index=[TABLE_TBL_POINTVALUE_10M_WTG_ID, TABLE_TBL_POINTVALUE_10M_DATATIME, CAL_THEORY_APPRODUCTION_10M,
               CAL_ACTIVE_APPRODUCTION_10M])])


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
    df_wtg.sort_index(by=[TABLE_TBL_POINTVALUE_10M_DATATIME], inplace=True)
    df_wtg[CAL_REAL_APPRODUCTION_10M] = df_wtg[TABLE_TBL_POINTVALUE_10M_APPRODUCTION] - df_wtg[
        TABLE_TBL_POINTVALUE_10M_APPRODUCTION].shift(1)

    # process decrease, out of max range, nan, previous nan condition
    df_wtg[CAL_REAL_APPRODUCTION_10M][~df_wtg[TABLE_IM_10M_CLEAN_CLEAN_FLAG].apply(
        lambda x: clean_flag_check_is_normal(x, TABLE_TBL_POINTVALUE_10M_APPRODUCTION))] = np.nan
    df_wtg[CAL_REAL_APPRODUCTION_10M_ERRORS] = df_wtg[TABLE_IM_10M_CLEAN_CLEAN_FLAG].apply(
        lambda x: clean_flag_get(x, TABLE_TBL_POINTVALUE_10M_APPRODUCTION))

    # filter no ntf wind speed
    condition = df_wtg[CAL_NTF_WIND_SPEED].isnull()
    df_wtg[CAL_REAL_APPRODUCTION_10M][condition] = np.nan
    df_wtg[CAL_REAL_APPRODUCTION_10M_ERRORS][condition] = df_wtg[CAL_REAL_APPRODUCTION_10M_ERRORS][condition].apply(
        lambda x: production_error_set(x, PRODUCTION_ERROR_NO_NTF_WIND_SPEED))

    # filter no ntf site wind speed
    condition = df_wtg[CAL_NTF_WIND_SPEED_SITE].isnull()
    df_wtg[CAL_REAL_APPRODUCTION_10M][condition] = np.nan
    df_wtg[CAL_REAL_APPRODUCTION_10M_ERRORS][condition] = df_wtg[CAL_REAL_APPRODUCTION_10M_ERRORS][condition].apply(
        lambda x: production_error_set(x, PRODUCTION_ERROR_NO_NTF_SITE_WIND_SPEED))

    # filter no ntf standard wind speed
    condition = df_wtg[CAL_NTF_WIND_SPEED_STANDARD].isnull()
    df_wtg[CAL_REAL_APPRODUCTION_10M][condition] = np.nan
    df_wtg[CAL_REAL_APPRODUCTION_10M_ERRORS][condition] = df_wtg[CAL_REAL_APPRODUCTION_10M_ERRORS][condition].apply(
        lambda x: production_error_set(x, PRODUCTION_ERROR_NO_NTF_STANDARD_WIND_SPEED))

    # filter no temperature
    condition = ~df_wtg[TABLE_IM_10M_CLEAN_CLEAN_FLAG].apply(
        lambda x: clean_flag_check_is_normal(x, TABLE_TBL_POINTVALUE_10M_TEMOUTAVE))
    df_wtg[CAL_REAL_APPRODUCTION_10M][condition] = np.nan
    df_wtg[CAL_REAL_APPRODUCTION_10M_ERRORS][condition] = df_wtg[CAL_REAL_APPRODUCTION_10M_ERRORS][condition].apply(
        lambda x: production_error_set(x, PRODUCTION_ERROR_NO_TEMP_OUT))

    # cal rp production, ap consumption, rp consumption
    df_wtg[CAL_REAL_RPPRODUCTION_10M] = df_wtg[TABLE_TBL_POINTVALUE_10M_RPPRODUCTION] - df_wtg[
        TABLE_TBL_POINTVALUE_10M_RPPRODUCTION].shift(1)
    df_wtg[CAL_REAL_APCONSUMED_10M] = df_wtg[TABLE_TBL_POINTVALUE_10M_APCONSUMED] - df_wtg[
        TABLE_TBL_POINTVALUE_10M_APCONSUMED].shift(1)
    df_wtg[CAL_REAL_RPCONSUMED_10M] = df_wtg[TABLE_TBL_POINTVALUE_10M_RPCONSUMED] - df_wtg[
        TABLE_TBL_POINTVALUE_10M_RPCONSUMED].shift(1)
    return df_wtg


def im_10m_cal_real():
    df_10m_clean, df_turbine, df_site, df_ntf, df_power_curve = read_data()
    df_ntf_all = cal_ntfs(df_10m_clean, df_turbine, df_ntf, df_site)
    df_clean_ntf_merge = pd.merge(df_10m_clean, df_ntf_all,
                                  on=[TABLE_TBL_POINTVALUE_10M_WTG_ID, TABLE_TBL_POINTVALUE_10M_DATATIME], how='left')
    wtg_groups = df_clean_ntf_merge.groupby([TABLE_TBL_POINTVALUE_10M_WTG_ID])
    df_cal_real = group_process(wtg_groups, cal_wtg_real_production)
    wtg_hour_groups = pd.merge(df_cal_real, df_power_curve, left_on=TABLE_TBL_POINTVALUE_10M_WTG_ID,
                               right_on=TABLE_IM_POWER_CURVE_WTG_ID).groupby(
        by=[TABLE_TBL_POINTVALUE_10M_WTG_ID, TABLE_TBL_POINTVALUE_10M_DATATIME])
    df_predict_productions = group_process(wtg_hour_groups, cal_predict_production)
    df_cal_all = pd.merge(df_cal_real, df_predict_productions, how='left')
    write_data(df_cal_all)


if __name__ == '__main__':
    im_10m_cal_real()
