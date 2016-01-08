# !/usr/bin/env python
import sys

SUCCESS = 0

KEY_MIN = "min"
KEY_MAX = "max"

NULL = "NULL"
NA = "N/A"
MAX_RECORD_NUMBER = 100000

records = []


def init():
    cleans = {}
    with open("wind_algo_fact_wtg_1m_clean_input_schema.conf") as sf:
        schema = sf.readline().strip().split(",")
    with open("wind_algo_fact_wtg_1m_clean.conf") as cf:
        for line in cf:
            field, min_value, max_value = line.strip().split(",")
            cleans[field] = {KEY_MIN: float(min_value), KEY_MAX: float(max_value) * 2}
    return schema, cleans


def output(r):
    global records
    records.append(r)
    if len(records) >= MAX_RECORD_NUMBER:
        batch_output()


def batch_output():
    global records
    for record in records:
        print record
    records[:] = []


if __name__ == "__main__":
    schema, cleans = init()
    for line in sys.stdin:
        values = line.strip().split(",")
        if len(values) != len(schema):
            print ("Wrong data format!")
            continue
        record = dict(zip(schema, values))
        for key in schema:
            value = record.get(key)
            if value == NA:
                continue
            elif key in cleans:
                fvalue = float(value)
                if fvalue > cleans[key][KEY_MAX] or fvalue < cleans[key][KEY_MIN]:
                    record[key] = NULL
        li = []
        for key in schema:
            li.append(record[key])
        output(",".join(li))
    batch_output()
