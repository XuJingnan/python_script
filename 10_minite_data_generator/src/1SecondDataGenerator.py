import datetime
import random

import time

from Constants import *


def generatorData(sec):
    data = []
    data.append(datetime.datetime.fromtimestamp(sec).strftime(SECOND_FORMAT))
    for j in range(0, NUM):
        data.append(str(random.random()))
    return data


def output(data, f):
    f.write(",".join(data) + "\n")
    f.flush()


if __name__ == "__main__":
    day = time.strftime(DAY_FORMAT, time.localtime())
    f = open(day + SECOND_OUTPUT_FILE_SUFFIX, 'a')
    sec = None
    while True:
        newDay = time.strftime(DAY_FORMAT, time.localtime())
        if day != newDay:
            f.close()
            f = open(newDay + SECOND_OUTPUT_FILE_SUFFIX, 'a')
        newSec = int(time.time())
        if newSec == sec:
            time.sleep(0.5)
            continue
        data = generatorData(newSec)
        output(data, f)
        sec = newSec
