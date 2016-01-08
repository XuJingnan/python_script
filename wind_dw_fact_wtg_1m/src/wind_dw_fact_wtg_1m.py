# !/usr/bin/env python
import datetime
import sys
import time

MAX_RECORD_NUMBER = 100000
records = []
NULL = "N/A"


def output(r):
    global records
    records.append(r)
    if len(records) >= MAX_RECORD_NUMBER:
        batch_output()


def batch_output():
    global records
    for r in records:
        print r
    records[:] = []


def process_date_time(date_time):
    if date_time == NULL:
        return NULL, NULL
    try:
        ts = time.mktime(datetime.datetime.strptime(date_time, "%Y-%m-%d %H:%M:%S").timetuple())
        return str(int(ts)), date_time.split(" ")[0].replace("-", "")
    except:
        return NULL, NULL


def get_read_wind_speed(fake_speed):
    if fake_speed == NULL:
        return fake_speed
    try:
        return str(float(record[key]) / 248.0)
    except:
        return NULL


# ################     process    ################
# key.lower()
# occur_time string transform to timestamp & date fields
if __name__ == "__main__":
    with open("wind_dw_fact_wtg_1m_input_schema.conf") as sf:
        schema = sf.readline().strip().split(",")
    for line in sys.stdin:
        record = {}
        broken_record = 0
        for kv in line.strip().split(","):
            tmp = kv.split(":")
            if len(tmp) == 2:
                record[tmp[0].lower()] = tmp[1]
            elif len(tmp) == 4:
                record[tmp[0].lower()] = ":".join([tmp[1], tmp[2], tmp[3]])
            else:
                broken_record = 1
                break
        if broken_record == 1:
            continue
        li = []
        for key in schema:
            key = key.lower()
            value = record.get(key, NULL)
            if key == "date":
                continue
            elif value == NULL:
                li.append(NULL)
            elif key == "occur_time":
                ts, date = process_date_time(value)
                li.append(ts)
                li.append(date)
            elif key == "readwindspeed":
                li.append(get_read_wind_speed(value))
            else:
                li.append(value)
        output(",".join(li))
    batch_output()
