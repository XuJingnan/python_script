from utils import *


# todo yongjun new cleaning rules
def read_data():
    clean_out_dir = os.sep.join([OUTPUT_DIR, execute_day_str, TABLE_IM_10M_CLEAN])
    rm_dirs(clean_out_dir)
    make_dirs(clean_out_dir)

    df_mapping = pd.read_csv(os.sep.join([INPUT_DIR, execute_day_str, TABLE_MDM_ALIAS_ENVID_MAPPING]))
    df_turbine = pd.read_csv(os.sep.join([INPUT_DIR, execute_day_str, IM_DIM_WIND_WTG]))
    df_today = pd.read_csv(os.sep.join([INPUT_DIR, execute_day_str, TABLE_TBL_POINTVALUE_10M]),
                           parse_dates=[TABLE_TBL_POINTVALUE_10M_DATATIME])

    yesterday_dir = os.sep.join([INPUT_DIR, UTIL_YESTERDAY, TABLE_IM_10M_CLEAN])
    make_dirs(yesterday_dir)
    yesterday_files = []
    for f in os.listdir(yesterday_dir):
        yesterday_files.append(
            pd.read_csv(os.sep.join([yesterday_dir, f]), parse_dates=[TABLE_TBL_POINTVALUE_10M_DATATIME]))
    if len(yesterday_files) > 0:
        df_yesterday = pd.concat(yesterday_files)
        # only get the execute day before last ten minutes records
        df_yesterday = df_yesterday[
            df_yesterday[TABLE_TBL_POINTVALUE_10M_DATATIME].apply(lambda x: is_execute_day_before_last_ten_minutes(x))]
    else:
        df_yesterday = pd.DataFrame(None)
    df_raw = pd.concat([df_yesterday, df_today])
    return df_raw[df_today.columns], df_mapping, df_turbine


def read_rules(rules_file):
    rules = {}
    with open(os.sep.join([CONFIG_DIR, rules_file])) as f:
        for line in f:
            if line is None or line.strip() == '' or line.startswith('#'):
                continue
            key, values = line.strip().split(':')
            min_value, max_value = values[1:-1].split(',')
            rules[key] = (float(min_value), float(max_value))
    return rules


def wind_speed_clean_more(df, key):
    df[TABLE_IM_10M_CLEAN_CLEAN_FLAG][df[key] == 0] = df[TABLE_IM_10M_CLEAN_CLEAN_FLAG].apply(
        lambda x: clean_flag_set(x, key, CLEAN_FLAG_TAG_OUT_OF_RANGE))
    df[TABLE_IM_10M_CLEAN_CLEAN_FLAG][
        df[TABLE_TBL_POINTVALUE_10M_WINDSPEEDSTD] / df[TABLE_TBL_POINTVALUE_10M_WINDSPEEDAVE] < 0.01] = df[
        TABLE_IM_10M_CLEAN_CLEAN_FLAG].apply(lambda x: clean_flag_set(x, key, CLEAN_FLAG_TAG_WIND_SPEED_FREEZE))
    return df


def production_clean_more(df):
    delta = df[TABLE_TBL_POINTVALUE_10M_APPRODUCTION] - df[TABLE_TBL_POINTVALUE_10M_APPRODUCTION].shift(1)
    # production null
    df[TABLE_IM_10M_CLEAN_CLEAN_FLAG][df[TABLE_TBL_POINTVALUE_10M_APPRODUCTION].isnull()] = df[
        TABLE_IM_10M_CLEAN_CLEAN_FLAG].apply(
        lambda x: clean_flag_set(x, TABLE_TBL_POINTVALUE_10M_APPRODUCTION, CLEAN_FLAG_TAG_PRODUCTION_NAN))
    # previous production null
    df[TABLE_IM_10M_CLEAN_CLEAN_FLAG][df[TABLE_TBL_POINTVALUE_10M_APPRODUCTION].notnull() & delta.isnull()] = df[
        TABLE_IM_10M_CLEAN_CLEAN_FLAG].apply(
        lambda x: clean_flag_set(x, TABLE_TBL_POINTVALUE_10M_APPRODUCTION, CLEAN_FLAG_TAG_PRODUCTION_NAN_PRE))
    # production decrease
    df[TABLE_IM_10M_CLEAN_CLEAN_FLAG][delta < 0] = df[TABLE_IM_10M_CLEAN_CLEAN_FLAG].apply(
        lambda x: clean_flag_set(x, TABLE_TBL_POINTVALUE_10M_APPRODUCTION, CLEAN_FLAG_TAG_PRODUCTION_DECREASE))
    # production out of max range
    df[TABLE_IM_10M_CLEAN_CLEAN_FLAG][delta > df[IM_DIM_WIND_WTG_RATED_POWER] / 6] = df[
        TABLE_IM_10M_CLEAN_CLEAN_FLAG].apply(
        lambda x: clean_flag_set(x, TABLE_TBL_POINTVALUE_10M_APPRODUCTION, CLEAN_FLAG_TAG_PRODUCTION_TOO_LARGE))
    return df


def fill_missing_data(turbine, df):
    first_time = df.index[0]
    last_time = df.index[-1]

    add_first, add_last = True, True
    if is_execute_day_before_last_ten_minutes(first_time) or is_execute_day_first_second(first_time):
        add_first = False
    if is_execute_day_last_ten_minutes(last_time):
        add_last = False
    if add_first:
        first_record = pd.DataFrame(None, index=pd.DatetimeIndex([execute_day_first_second]))
        df = pd.concat([first_record, df])
    columns = df.columns
    if add_last:
        last_record = pd.DataFrame(None, index=pd.DatetimeIndex([execute_day_last_ten_minutes]))
        df = pd.concat([df, last_record])
    df = df.asfreq('10Min')
    df[TABLE_TBL_POINTVALUE_10M_WTG_ID] = turbine
    df[TABLE_TBL_POINTVALUE_10M_DATATIME] = df.index
    return df[columns]


def save_last_record(turbine, last):
    last.to_csv(os.sep.join([INPUT_DIR, UTIL_YESTERDAY, TABLE_IM_10M_CLEAN, turbine]), index=False)


def other_condition_check(df):
    # active power check
    null_condition = df[TABLE_TBL_POINTVALUE_10M_ACTIVEPWAVE].isnull()
    df = set_null(df, null_condition, TABLE_TBL_POINTVALUE_10M_ACTIVEPWAVE)
    out_of_rang_condition = (df[TABLE_TBL_POINTVALUE_10M_ACTIVEPWAVE] < -0.5 * df[IM_DIM_WIND_WTG_RATED_POWER]) | (
        df[TABLE_TBL_POINTVALUE_10M_ACTIVEPWAVE] > 2 * df[IM_DIM_WIND_WTG_RATED_POWER])
    df = set_out_of_range(df, out_of_rang_condition, TABLE_TBL_POINTVALUE_10M_ACTIVEPWAVE)

    # torque set point check
    null_condition = df[TABLE_TBL_POINTVALUE_10M_TORQUESETPOINTAVE].isnull()
    df = set_null(df, null_condition, TABLE_TBL_POINTVALUE_10M_TORQUESETPOINTAVE)
    out_of_rang_condition = (df[TABLE_TBL_POINTVALUE_10M_TORQUESETPOINTAVE] < 0) | (
        df[TABLE_TBL_POINTVALUE_10M_TORQUESETPOINTAVE] > 2 * df[IM_DIM_WIND_WTG_RATED_TORQUE])
    df = set_out_of_range(df, out_of_rang_condition, TABLE_TBL_POINTVALUE_10M_TORQUESETPOINTAVE)

    # torque check
    null_condition = df[TABLE_TBL_POINTVALUE_10M_TORQUEAVE].isnull()
    df = set_null(df, null_condition, TABLE_TBL_POINTVALUE_10M_TORQUEAVE)
    out_of_rang_condition = (df[TABLE_TBL_POINTVALUE_10M_TORQUEAVE] < 0) | (
        df[TABLE_TBL_POINTVALUE_10M_TORQUEAVE] > 2 * df[IM_DIM_WIND_WTG_RATED_TORQUE])
    df = set_out_of_range(df, out_of_rang_condition, TABLE_TBL_POINTVALUE_10M_TORQUEAVE)

    # rotor speed check
    null_condition = df[TABLE_TBL_POINTVALUE_10M_ROTORSPDAVE].isnull()
    df = set_null(df, null_condition, TABLE_TBL_POINTVALUE_10M_ROTORSPDAVE)
    out_of_rang_condition = (df[TABLE_TBL_POINTVALUE_10M_ROTORSPDAVE] < -0.5 * df[
        IM_DIM_WIND_WTG_ON_GRID_ROTOR_SPEED]) | (
                                df[TABLE_TBL_POINTVALUE_10M_ROTORSPDAVE] > 2 * df[IM_DIM_WIND_WTG_ON_GRID_ROTOR_SPEED])
    df = set_out_of_range(df, out_of_rang_condition, TABLE_TBL_POINTVALUE_10M_ROTORSPDAVE)

    # generator speed check
    null_condition = df[TABLE_TBL_POINTVALUE_10M_GENSPDAVE].isnull()
    df = set_null(df, null_condition, TABLE_TBL_POINTVALUE_10M_GENSPDAVE)
    out_of_rang_condition = (df[TABLE_TBL_POINTVALUE_10M_GENSPDAVE] < -0.5 * df[
        IM_DIM_WIND_WTG_ON_GRID_GENERATOR_SPEED]) | (
                                df[TABLE_TBL_POINTVALUE_10M_GENSPDAVE] > 2 * df[
                                    IM_DIM_WIND_WTG_ON_GRID_GENERATOR_SPEED])
    df = set_out_of_range(df, out_of_rang_condition, TABLE_TBL_POINTVALUE_10M_GENSPDAVE)

    return df


def write_date(turbine, df):
    out_dir = os.sep.join([OUTPUT_DIR, execute_day_str, TABLE_IM_10M_CLEAN])
    make_dirs(out_dir)
    columns = [TABLE_TBL_POINTVALUE_10M_WTG_ID, TABLE_TBL_POINTVALUE_10M_DATATIME, TABLE_TBL_POINTVALUE_10M_TEMOUTAVE,
               TABLE_TBL_POINTVALUE_10M_WINDDIRECTIONAVE, TABLE_TBL_POINTVALUE_10M_NACELLEPOSITIONAVE,
               TABLE_TBL_POINTVALUE_10M_BLADEPITCHAVE, TABLE_TBL_POINTVALUE_10M_WINDSPEEDAVE,
               TABLE_TBL_POINTVALUE_10M_WINDSPEEDSTD, TABLE_TBL_POINTVALUE_10M_ROTORSPDAVE,
               TABLE_TBL_POINTVALUE_10M_GENSPDAVE, TABLE_TBL_POINTVALUE_10M_TORQUESETPOINTAVE,
               TABLE_TBL_POINTVALUE_10M_TORQUEAVE, TABLE_TBL_POINTVALUE_10M_ACTIVEPWAVE,
               TABLE_TBL_POINTVALUE_10M_PCURVESTSAVE, TABLE_TBL_POINTVALUE_10M_APPRODUCTION,
               TABLE_TBL_POINTVALUE_10M_RPPRODUCTION, TABLE_TBL_POINTVALUE_10M_APCONSUMED,
               TABLE_TBL_POINTVALUE_10M_RPCONSUMED, TABLE_IM_10M_CLEAN_CLEAN_FLAG]
    df[columns].to_csv(os.sep.join([out_dir, turbine]), index=False)

    with open(os.sep.join([out_dir, IM_TURBINES]), 'a') as f:
        f.write(str(turbine) + '\n')


# out a turbine 144 or 145(add previous record last day) records
# @profile
def clean_data(df):
    turbine = df.iloc[0][TABLE_TBL_POINTVALUE_10M_WTG_ID]
    df = df.sort_index(by=TABLE_TBL_POINTVALUE_10M_DATATIME)
    df = df.set_index(pd.DatetimeIndex(df[TABLE_TBL_POINTVALUE_10M_DATATIME]))

    save_last_record(turbine, df.iloc[-1:])

    df = fill_missing_data(turbine, df)
    rules = read_rules('clean_rules')

    df[TABLE_TBL_POINTVALUE_10M_NACELLEPOSITIONAVE] %= 360
    df[TABLE_TBL_POINTVALUE_10M_TEMOUTAVE] += CONSTANT_TEMPERATURE
    df[TABLE_IM_10M_CLEAN_CLEAN_FLAG] = 0

    for key, values in rules.iteritems():
        # null check
        df[TABLE_IM_10M_CLEAN_CLEAN_FLAG][pd.isnull(df[key])] = df[TABLE_IM_10M_CLEAN_CLEAN_FLAG].apply(
            lambda x: clean_flag_set(x, key, CLEAN_FLAG_TAG_NULL))
        # out of range check
        df[TABLE_IM_10M_CLEAN_CLEAN_FLAG][
            (key != TABLE_TBL_POINTVALUE_10M_APPRODUCTION) & ((df[key] < values[0]) | (df[key] > values[1]))] = df[
            TABLE_IM_10M_CLEAN_CLEAN_FLAG].apply(lambda x: clean_flag_set(x, key, CLEAN_FLAG_TAG_OUT_OF_RANGE))
        # more wind speed check
        if key == TABLE_TBL_POINTVALUE_10M_WINDSPEEDAVE:
            df = wind_speed_clean_more(df, key)

    # more production check
    df = production_clean_more(df)

    # other condition check
    df = other_condition_check(df)

    df[TABLE_IM_10M_CLEAN_CLEAN_FLAG][df[TABLE_TBL_POINTVALUE_10M_WTG_ID].isnull()] = df[
        TABLE_IM_10M_CLEAN_CLEAN_FLAG].apply(
        lambda x: clean_flag_set(0, TABLE_TBL_POINTVALUE_10M_MISSING, CLEAN_FLAG_TAG_NULL))
    df[TABLE_TBL_POINTVALUE_10M_DATATIME] = df.index
    df[TABLE_TBL_POINTVALUE_10M_WTG_ID] = df.iloc[0][TABLE_TBL_POINTVALUE_10M_WTG_ID]
    write_date(turbine, df)


# @profile
def im_10m_clean():
    df_raw, df_mapping, df_turbine = read_data()
    df_raw = pd.merge(df_raw, df_mapping, left_on=TABLE_TBL_POINTVALUE_10M_WTG_ID,
                      right_on=TABLE_MDM_ALIAS_ENVID_MAPPING_IDX)
    df_raw[TABLE_TBL_POINTVALUE_10M_WTG_ID] = df_raw[TABLE_MDM_ALIAS_ENVID_MAPPING_MDM_ID]
    df_raw = pd.merge(df_raw, df_turbine, left_on=TABLE_TBL_POINTVALUE_10M_WTG_ID, right_on=IM_DIM_WIND_WTG_WTG_ID)
    groups = df_raw.groupby(TABLE_TBL_POINTVALUE_10M_WTG_ID)
    groups.apply(clean_data)


if __name__ == '__main__':
    im_10m_clean()
