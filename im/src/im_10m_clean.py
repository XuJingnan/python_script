import datetime
import numpy as np
import pandas as pd

from utils import *


def read_data():
    # todo read yesterday last production
    return pd.read_csv(INPUT_DIR + TABLE_TBL_POINTVALUE_10M + '.' + str(datetime.datetime.now().strftime('%Y-%m-%d')))


def read_rules(rules_file):
    rules = {}
    with open(CONFIG_DIR + rules_file) as f:
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
        df[TABLE_TBL_POINTVALUE_10M_READWINDSPEEDSTD] / df[TABLE_TBL_POINTVALUE_10M_READWINDSPEEDAVE] < 0.01] = df[
        TABLE_IM_10M_CLEAN_CLEAN_FLAG].apply(lambda x: clean_flag_set(x, key, CLEAN_FLAG_TAG_WIND_SPEED_FREEZE))
    return df


def production_clean_more(df, key, values):
    delta = df[TABLE_TBL_POINTVALUE_10M_APPRODUCTION] - df[TABLE_TBL_POINTVALUE_10M_APPRODUCTION].shift(1)
    # production null
    df[TABLE_IM_10M_CLEAN_CLEAN_FLAG][df[TABLE_TBL_POINTVALUE_10M_APPRODUCTION].isnull()] = df[
        TABLE_IM_10M_CLEAN_CLEAN_FLAG].apply(lambda x: clean_flag_set(x, key, CLEAN_FLAG_TAG_PRODUCTION_NAN))
    # previous production null
    df[TABLE_IM_10M_CLEAN_CLEAN_FLAG][(~df[TABLE_TBL_POINTVALUE_10M_APPRODUCTION].isnull()) & delta.isnull()] = df[
        TABLE_IM_10M_CLEAN_CLEAN_FLAG].apply(lambda x: clean_flag_set(x, key, CLEAN_FLAG_TAG_PRODUCTION_NAN_PRE))
    # production decrease
    df[TABLE_IM_10M_CLEAN_CLEAN_FLAG][delta < values[0]] = df[TABLE_IM_10M_CLEAN_CLEAN_FLAG].apply(
        lambda x: clean_flag_set(x, key, CLEAN_FLAG_TAG_PRODUCTION_DECREASE))
    # production too large
    df[TABLE_IM_10M_CLEAN_CLEAN_FLAG][delta > values[1]] = df[TABLE_IM_10M_CLEAN_CLEAN_FLAG].apply(
        lambda x: clean_flag_set(x, key, CLEAN_FLAG_TAG_PRODUCTION_TOO_LARGE))
    return df


def clean_data(df):
    df.sort_index(by=TABLE_TBL_POINTVALUE_10M_DATATIME, inplace=True)
    rules = read_rules('clean_rules')

    df[TABLE_TBL_POINTVALUE_10M_NACELLEPOSITIONAVE] %= 360

    df[TABLE_IM_10M_CLEAN_CLEAN_FLAG] = pd.Series(np.zeros(len(df.index), dtype=np.int64), index=df.index)
    for key, values in rules.iteritems():
        # null check
        df[TABLE_IM_10M_CLEAN_CLEAN_FLAG][pd.isnull(df[key])] = df[TABLE_IM_10M_CLEAN_CLEAN_FLAG].apply(
            lambda x: clean_flag_set(x, key, CLEAN_FLAG_TAG_NULL))
        # out of range check
        df[TABLE_IM_10M_CLEAN_CLEAN_FLAG][
            (key != TABLE_TBL_POINTVALUE_10M_APPRODUCTION) & ((df[key] < values[0]) | (df[key] > values[1]))] = df[
            TABLE_IM_10M_CLEAN_CLEAN_FLAG].apply(
            lambda x: clean_flag_set(x, key, CLEAN_FLAG_TAG_OUT_OF_RANGE))

        # more speed check
        if key == TABLE_TBL_POINTVALUE_10M_READWINDSPEEDAVE:
            df = wind_speed_clean_more(df, key)
        # more production check
        if key == TABLE_TBL_POINTVALUE_10M_APPRODUCTION:
            df = production_clean_more(df, key, values)
    return df


def write_date(df):
    df.to_csv(OUTPUT_DIR + TABLE_IM_10M_CLEAN + '.' + str(datetime.datetime.now().strftime('%Y-%m-%d')))


def im_10m_clean():
    df = read_data()
    df = df.groupby(TABLE_TBL_POINTVALUE_10M_WTG_ID).apply(lambda x: clean_data(x))
    write_date(df)


if __name__ == '__main__':
    im_10m_clean()
