import datetime
import numpy as np
import os
import pandas as pd

# constant for calculate air density
CONSTANT_TEMPERATURE = 273.15
CONSTANT_GAS_DRY_AIR = 287.05
CONSTANT_GAS_WATER_VAPOUR = 461.5
CONSTANT_RELATIVE_HUMIDITY = 0.5
CONSTANT_STANDARD_AIR_DENSITY = 1.225

# some directory
INPUT_DIR = 'input'
UTIL_YESTERDAY = 'util_yesterday'
CONFIG_DIR = 'config'
OUTPUT_DIR = 'output'

# some input table
## STG_FACT_WTG_10M
STG_FACT_WTG_10M = 'STG_FACT_WTG_10M'
STG_FACT_WTG_10M_WTG_ID = 'WTG_ID'
STG_FACT_WTG_10M_DATATIME = 'DATATIME'
STG_FACT_WTG_10M_TEMOUTAVE = 'TEMOUTAVE'
STG_FACT_WTG_10M_WINDDIRECTIONAVE = 'WINDDIRECTIONAVE'
STG_FACT_WTG_10M_NACELLEPOSITIONAVE = 'NACELLEPOSITIONAVE'
STG_FACT_WTG_10M_BLADEPITCHAVE = 'BLADEPITCHAVE'
STG_FACT_WTG_10M_WINDSPEEDAVE = 'WINDSPEEDAVE'
STG_FACT_WTG_10M_WINDSPEEDSTD = 'WINDSPEEDSTD'
STG_FACT_WTG_10M_ROTORSPDAVE = 'ROTORSPDAVE'
STG_FACT_WTG_10M_GENSPDAVE = 'GENSPDAVE'
STG_FACT_WTG_10M_TORQUESETPOINTAVE = 'TORQUESETPOINTAVE'
STG_FACT_WTG_10M_TORQUEAVE = 'TORQUEAVE'
STG_FACT_WTG_10M_ACTIVEPWAVE = 'ACTIVEPWAVE'
STG_FACT_WTG_10M_PCURVESTSAVE = 'PCURVESTSAVE'
STG_FACT_WTG_10M_APPRODUCTION = 'APPRODUCTION'
STG_FACT_WTG_10M_RPPRODUCTION = 'RPPRODUCTION'
STG_FACT_WTG_10M_APCONSUMED = 'APCONSUMED'
STG_FACT_WTG_10M_RPCONSUMED = 'RPCONSUMED'

STG_FACT_WTG_10M_MISSING = 'MISSING'

## DIM_WTG_FULL
DIM_WTG_FULL = 'DIM_WTG_FULL'
DIM_WTG_FULL_WTG_ID = 'WTG_ID'
DIM_WTG_FULL_SITE_ID = 'SITE_ID'
DIM_WTG_FULL_ALTITUDE = 'ALTITUDE'
DIM_WTG_FULL_HUB_HEIGHT = 'HUB_HEIGHT'
DIM_WTG_FULL_SCADA_NTF_ID = 'SCADA_NTF_ID'
DIM_WTG_FULL_RATED_POWER = 'RATED_POWER'
DIM_WTG_FULL_RATED_TORQUE = 'RATED_TORQUE'
DIM_WTG_FULL_ON_GRID_ROTOR_SPEED = 'ON_GRID_ROTOR_SPEED'
DIM_WTG_FULL_ON_GRID_GENERATOR_SPEED = 'ON_GRID_GENERATOR_SPEED'

## DIM_SITE_FULL
DIM_SITE_FULL = 'DIM_SITE_FULL'
DIM_SITE_FULL_SITE_ID = 'SITE_ID'
DIM_SITE_FULL_AIR_DENSITY = 'AIR_DENSITY'

## DIM_NTF_DATA
DIM_NTF_DATA = 'DIM_NTF_DATA'
DIM_NTF_DATA_NTF_ID = 'NTF_ID'
DIM_NTF_DATA_ROTOR_SPEED = 'ROTOR_SPEED'
DIM_NTF_DATA_A = 'A'
DIM_NTF_DATA_B = 'B'
DIM_NTF_DATA_C = 'C'

## DIM_WTG_PC
DIM_WTG_PC = 'DIM_WTG_PC'
DIM_WTG_PC_WTG_ID = 'WTG_ID'
DIM_WTG_PC_PC_TYPE = 'PC_TYPE'
DIM_WTG_PC_PC_ID = 'PC_ID'

## DIM_PC_DATA
DIM_PC_DATA = 'DIM_PC_DATA'
DIM_PC_DATA_PC_ID = 'PC_ID'
DIM_PC_DATA_WIND_SPEED_RANK = 'WIND_SPEED_RANK'
DIM_PC_DATA_POWER = 'POWER'

## STG_FACT_NO_CONN
STG_FACT_NO_CONN = 'STG_FACT_NO_CONN'
STG_FACT_NO_CONN_WTG_ID = 'WTG_ID'
STG_FACT_NO_CONN_NC_STARTTIME = 'NC_STARTTIME'
STG_FACT_NO_CONN_ID = 'NC_ID'

## STG_FACT_STANDARD_STATE
STG_FACT_STANDARD_STATE = 'STG_FACT_STANDARD_STATE'
STG_FACT_STANDARD_STATE_WTG_ID = 'WTG_ID'
STG_FACT_STANDARD_STATE_STARTTIME = 'SS_STARTTIME'
STG_FACT_STANDARD_STATE_ID = 'SS_ID'

## DIM_ALIAS_ENVID_MAPPING
DIM_ALIAS_ENVID_MAPPING = 'DIM_ALIAS_ENVID_MAPPING'
DIM_ALIAS_ENVID_MAPPING_IDX = 'IDX'
DIM_ALIAS_ENVID_MAPPING_MDM_ID = 'MDM_ID'

# some output tables

## TABLE_IM_10M_CLEAN
TABLE_IM_10M_CLEAN = 'IM_10M_CLEAN'
TABLE_IM_10M_CLEAN_CLEAN_FLAG = 'CLEAN_FLAG'

CLEAN_FLAG_TAG_NORMAL = 0
CLEAN_FLAG_TAG_NULL = 1
CLEAN_FLAG_TAG_OUT_OF_RANGE = 2
CLEAN_FLAG_TAG_WIND_SPEED_FREEZE = 4
# todo STG_FACT_WTG_10M_WINDSPEEDSTD / STG_FACT_WTG_10M_WINDSPEEDAVE too big ?
CLEAN_FLAG_TAG_PRODUCTION_DECREASE = 1
CLEAN_FLAG_TAG_PRODUCTION_TOO_LARGE = 2
CLEAN_FLAG_TAG_PRODUCTION_NAN = 4
CLEAN_FLAG_TAG_PRODUCTION_NAN_PRE = 8

CLEAN_FLAG_MAPS = {
    STG_FACT_WTG_10M_TEMOUTAVE: 0,
    STG_FACT_WTG_10M_WINDDIRECTIONAVE: 1,
    STG_FACT_WTG_10M_BLADEPITCHAVE: 2,
    STG_FACT_WTG_10M_WINDSPEEDAVE: 3,
    STG_FACT_WTG_10M_ROTORSPDAVE: 4,
    STG_FACT_WTG_10M_GENSPDAVE: 5,
    STG_FACT_WTG_10M_TORQUESETPOINTAVE: 6,
    STG_FACT_WTG_10M_TORQUEAVE: 7,
    STG_FACT_WTG_10M_ACTIVEPWAVE: 8,
    STG_FACT_WTG_10M_PCURVESTSAVE: 9,
    STG_FACT_WTG_10M_APPRODUCTION: 10,
    STG_FACT_WTG_10M_MISSING: 11
}

## TABLE_IM_10M_CAL_REAL
TABLE_IM_10M_CAL_REAL = 'IM_10M_CAL_REAL'
CAL_AIR_DENSITY = 'CAL_AIR_DENSITY'
CAL_NTF_WIND_SPEED = 'CAL_NTF_WIND_SPEED'
CAL_NTF_WIND_SPEED_STANDARD = 'CAL_NTF_WIND_SPEED_STANDARD'
CAL_NTF_WIND_SPEED_SITE = 'CAL_NTF_WIND_SPEED_SITE'

CAL_REAL_APPRODUCTION_10M = 'CAL_REAL_APPRODUCTION_10M'
CAL_REAL_APPRODUCTION_10M_ERRORS = 'CAL_REAL_APPRODUCTION_10M_ERRORS'
CAL_THEORY_APPRODUCTION_10M = 'CAL_THEORY_APPRODUCTION_10M'
CAL_ACTIVE_APPRODUCTION_10M = 'CAL_ACTIVE_APPRODUCTION_10M'

CAL_REAL_RPPRODUCTION_10M = 'CAL_REAL_RPPRODUCTION_10M'
CAL_REAL_APCONSUMED_10M = 'CAL_REAL_APCONSUMED_10M'
CAL_REAL_RPCONSUMED_10M = 'CAL_REAL_RPCONSUMED_10M'

PRODUCTION_ERROR_DECREASE = 'DECREASE'
PRODUCTION_ERROR_OUT_OF_MAX_RANGE = 'OUT_OF_MAX_RANGE'
PRODUCTION_ERROR_NULL = 'NAN'
PRODUCTION_ERROR_NAN_PRE = 'NAN_PRE'
PRODUCTION_ERROR_NO_NTF_WIND_SPEED = 'NO_NTF_WIND_SPEED'
PRODUCTION_ERROR_NO_NTF_SITE_WIND_SPEED = 'NO_NTF_SITE_WIND_SPEED'
PRODUCTION_ERROR_NO_NTF_STANDARD_WIND_SPEED = 'NO_NTF_STANDARD_WIND_SPEED'
PRODUCTION_ERROR_NO_TEMP_OUT = 'NO_TEMP_OUT'

CAL_REAL_PRODUCTION_10M_ERRORS_MAP = {
    PRODUCTION_ERROR_DECREASE: 0,
    PRODUCTION_ERROR_OUT_OF_MAX_RANGE: 1,
    PRODUCTION_ERROR_NULL: 2,
    PRODUCTION_ERROR_NAN_PRE: 3,
    PRODUCTION_ERROR_NO_NTF_WIND_SPEED: 4,
    PRODUCTION_ERROR_NO_NTF_SITE_WIND_SPEED: 5,
    PRODUCTION_ERROR_NO_NTF_STANDARD_WIND_SPEED: 6,
    PRODUCTION_ERROR_NO_TEMP_OUT: 7
}

## TABLE_IM_STATE_ALL
TABLE_IM_STATE_ALL = 'IM_STATE_ALL'

## TABLE_IM_PRODUCTION_BASE
TABLE_IM_PRODUCTION_BASE = 'IM_PRODUCTION_BASE'
TABLE_IM_PRODUCTION_BASE_PRODUCTION_PER_SECOND = 'PRODUCTION_PER_SECOND'

# some output file
IM_TURBINES = 'IM_TURBINES'
IM_STATE_ALL_NC_TURBINE_LAST_RECORD = 'IM_STATE_ALL_NC_TURBINE_LAST_RECORD'
IM_STATE_ALL_SS_TURBINE_LAST_RECORD = 'IM_STATE_ALL_SS_TURBINE_LAST_RECORD'

## some output table
DW_TMP_FACT_WTG_10M_CLEAN = 'DW_TMP_FACT_WTG_10M_CLEAN'

# some functions
def clean_flag_set(clean_flag, clean_flag_field, clean_flag_condition):
    clean_flag += clean_flag_condition << (CLEAN_FLAG_MAPS.get(clean_flag_field) * 4)
    return clean_flag


def clean_flag_check_is_normal(clean_flag, clean_flag_field):
    return clean_flag_get(clean_flag, clean_flag_field) == 0


def clean_flag_get(clean_flag, clean_flag_field):
    return (clean_flag & (0b1111 << (CLEAN_FLAG_MAPS.get(clean_flag_field) * 4))) >> \
           (CLEAN_FLAG_MAPS.get(clean_flag_field) * 4)


def set_null(df, condition, field):
    df[TABLE_IM_10M_CLEAN_CLEAN_FLAG][condition] = df[TABLE_IM_10M_CLEAN_CLEAN_FLAG].apply(
        lambda x: clean_flag_set(x, field, CLEAN_FLAG_TAG_NULL))
    return df


def set_out_of_range(df, condition, field):
    df[TABLE_IM_10M_CLEAN_CLEAN_FLAG][condition] = df[TABLE_IM_10M_CLEAN_CLEAN_FLAG].apply(
        lambda x: clean_flag_set(x, field, CLEAN_FLAG_TAG_OUT_OF_RANGE))
    return df


def set_column_nan(df, column):
    df[column] = np.nan


def clean_flag_to_string(clean_flag):
    for key in CLEAN_FLAG_MAPS:
        value = clean_flag_get(clean_flag, key)
        if value != 0:
            print key, ':',
            if key != STG_FACT_WTG_10M_APPRODUCTION and value & CLEAN_FLAG_TAG_NULL:
                print 'null,',
            if key != STG_FACT_WTG_10M_APPRODUCTION and value & CLEAN_FLAG_TAG_OUT_OF_RANGE:
                print 'out of range,',
            if key == STG_FACT_WTG_10M_WINDSPEEDAVE and value & CLEAN_FLAG_TAG_WIND_SPEED_FREEZE:
                print 'wind speed freeze,',
            if key == STG_FACT_WTG_10M_APPRODUCTION:
                if value & CLEAN_FLAG_TAG_PRODUCTION_NAN:
                    print 'null',
                if value & CLEAN_FLAG_TAG_PRODUCTION_NAN_PRE:
                    print 'pre null',
                if value & CLEAN_FLAG_TAG_PRODUCTION_DECREASE:
                    print 'decrease',
                if value & CLEAN_FLAG_TAG_PRODUCTION_TOO_LARGE:
                    print 'too large',
    print ''


def production_error_set(flag, type):
    flag += 0b1 << CAL_REAL_PRODUCTION_10M_ERRORS_MAP.get(type)
    return flag


def production_error_to_string(flag):
    for key in CAL_REAL_PRODUCTION_10M_ERRORS_MAP:
        if flag & (0b1 << CAL_REAL_PRODUCTION_10M_ERRORS_MAP.get(key)):
            print key, ',',


def group_process(grouped, agg, concat=False):
    d = dict(list(grouped))
    if concat is False:
        for key, value in d.iteritems():
            agg(key, value)
        return None
    else:
        arr = [None] * len(d)
        i = 0
        for key, value in d.iteritems():
            arr[i] = agg(key, value)
            i += 1
        return pd.concat(arr)


def read_all_turbines():
    turbines = []
    date = execute_day_str
    with open(os.sep.join([OUTPUT_DIR, date, TABLE_IM_10M_CLEAN, IM_TURBINES])) as f:
        for line in f:
            turbines.append(line.strip())
    return turbines


def make_dirs(path):
    mk_cmd = 'mkdir -p {path}'.format(path=path)
    os.system(mk_cmd)


def rm_dirs(path):
    rm_cmd = 'rm -rf {path}'.format(path=path)
    os.system(rm_cmd)


# todo debug today
# today = datetime.datetime.now()
today = datetime.datetime(year=2016, month=3, day=11, hour=0, minute=0, second=0, microsecond=0)
execute_day = today - datetime.timedelta(days=1)
execute_day_before = execute_day - datetime.timedelta(days=1)

today_str = today.strftime('%Y-%m-%d')
execute_day_str = execute_day.strftime('%Y-%m-%d')
execute_day_before_str = execute_day_before.strftime('%Y-%m-%d')

execute_day_first_second = execute_day.replace(hour=0, minute=0, second=0, microsecond=0)
execute_day_last_ten_minutes = execute_day.replace(hour=23, minute=50, second=0, microsecond=0)
execute_day_last_second = execute_day.replace(hour=23, minute=59, second=59, microsecond=0)


def is_execute_day_before_more(d):
    return d < execute_day


def is_execute_day_before(d):
    return d.year == execute_day_before.year and d.month == execute_day_before.month and d.day == execute_day_before.day


def is_execute_day_first_second(d):
    return d == execute_day_first_second


def is_execute_day_last_second(d):
    return d == execute_day_last_second


def is_execute_day_before_last_ten_minutes(d):
    return is_execute_day_before(d) and d.hour == 23 and d.minute == 50 and d.second == 0


def is_execute_day_last_ten_minutes(d):
    return d.day == execute_day.day and d.hour == 23 and d.minute == 50 and d.second == 0

# clean_flag_to_string(8796093030400)
# print clean_flag_set(0, STG_FACT_WTG_10M_MISSING, CLEAN_FLAG_TAG_NULL)
# production_error_to_string(112)
# print first_time_is_yesterday(datetime.datetime(2016, 03, 22))
# print first_second
