import numpy as np
import pandas as pd

from utils import *


def read_data():
    # todo get production of index: -1, yesterday
    return pd.read_csv('test.data.csv')


def read_rules(rules_file):
    rules = {}
    with open(rules_file) as f:
        for line in f:
            if line is None or line.strip() == '' or line.startswith('#'):
                continue
            key, values = line.strip().split(':')
            min_value, max_value = values[1:-1].split(',')
            rules[key] = (float(min_value), float(max_value))
    return rules


def clean_data(df):
    rules = read_rules('clean_rules')

    df[CLEAN_FLAG_COLUMN_NACELLE_POSITION] %= 360

    df[CLEAN_FLAG] = pd.Series(np.zeros(len(df.index), dtype=np.int64), index=df.index)
    for key, values in rules.iteritems():
        df[CLEAN_FLAG][pd.isnull(df[key])] = df[CLEAN_FLAG].apply(
            lambda x: clean_flag_set(x, key, CLEAN_FLAG_TAG_NULL))
        df[CLEAN_FLAG][(df[key] < values[0]) | (df[key] > values[1])] = df[CLEAN_FLAG].apply(
            lambda x: clean_flag_set(x, key, CLEAN_FLAG_TAG_OUT_OF_RANGE))
        if key == CLEAN_FLAG_COLUMN_WIND_SPEED:
            df[CLEAN_FLAG][df[key] == values[0]] = df[CLEAN_FLAG].apply(
                lambda x: clean_flag_set(x, key, CLEAN_FLAG_TAG_OUT_OF_RANGE))
            df[CLEAN_FLAG][df[CLEAN_FLAG_COLUMN_WIND_SPEED_STD] / df[CLEAN_FLAG_COLUMN_WIND_SPEED] < 0.01] = df[
                CLEAN_FLAG].apply(lambda x: clean_flag_set(x, key, CLEAN_FLAG_TAG_WIND_SPEED_FREEZE))
        if key == CLEAN_FLAG_COLUMN_PRODUCTION:
            df[CLEAN_FLAG][
                ~(df[CLEAN_FLAG_COLUMN_PRODUCTION] - df[CLEAN_FLAG_COLUMN_PRODUCTION].shift(1) >= 0)] = \
                df[CLEAN_FLAG].apply(lambda x: clean_flag_set(x, key, CLEAN_FLAG_TAG_PRODUCTION_DECREASE))


def write_date(df):
    df.to_csv('im_10m_clean.out.csv')


def im_10m_clean():
    df = read_data()
    clean_data(df)
    write_date(df)


if __name__ == '__main__':
    im_10m_clean()
