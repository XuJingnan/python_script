import datetime
import sys
import threading

import time


def generate_all_timestamp(cycle, start_timestamp, end_timestamp):
    all_timestamps = []
    start = start_timestamp
    step = 0
    if cycle == "D":
        step = 24 * 60 * 60
    elif cycle == "H":
        step = 60 * 60
    while start <= end_timestamp:
        all_timestamps.append(start)
        start += step
    return all_timestamps


def generate_all_parameters(script_parameters, all_timestamps):
    all_parameters = []
    for t in all_timestamps:
        all_parameters.append(script_parameters.replace("timestamps", str(t)).split(" "))
    return all_parameters


def batch_process(threads):
    for t in threads:
        t.start()
    for t in threads:
        t.join()


def generate_cmd(args):
    script_name = args[0]
    module = __import__(script_name)
    methed = getattr(module, args[1])
    cycle = args[2]
    start_timestamps = int(time.mktime(datetime.datetime.strptime(args[3], "%Y-%m-%d %H:%M:%S").timetuple()))
    end_timestamps = int(time.mktime(datetime.datetime.strptime(args[4], "%Y-%m-%d %H:%M:%S").timetuple()))
    parallel = int(args[5])
    script_parameters = " ".join(args[6:])
    all_timestamps = generate_all_timestamp(cycle, start_timestamps, end_timestamps)
    all_parameters = generate_all_parameters(script_parameters, all_timestamps)

    threads = []
    cnt = 0
    for p in all_parameters:
        cnt += 1
        threads.append(threading.Thread(target=methed, args=tuple(p), ))
        if cnt % parallel == 0:
            batch_process(threads)
            threads = []
    batch_process(threads)


# python run_history.py script_name exec_func cycle start_time end_time script_parameters
if __name__ == "__main__":
    generate_cmd(sys.argv[1:])
