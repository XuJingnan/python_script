import commands
import datetime
import sys
import time

KEY_TASK_NAME = "task_name"
KEY_TASK_OBJ = "task_obj"
KEY_PARA1 = "para1"
KEY_CYCLE = "cycle"
SUCCESS = 0
TIME_FORMAT = "%Y-%m-%d %H:%M:%S"


def init():
    tasks = {}
    with open("mysql_task_conf.csv") as f:
        for line in f:
            task_id, task_obj, para1, cycle = line.strip().split(",")
            tasks[task_id] = {KEY_TASK_OBJ: task_obj, KEY_PARA1: para1, KEY_CYCLE: cycle}
    return tasks


def get_timestamp(time_str, format):
    return int(time.mktime(datetime.datetime.strptime(time_str, format).timetuple()))


def run(argv, tasks):
    task_id = argv[0]
    configurations = tasks[task_id]
    start = get_timestamp(argv[1], TIME_FORMAT)
    end = get_timestamp(argv[2], TIME_FORMAT)
    if configurations[KEY_CYCLE] == 'D':
        step = 24 * 60 * 60
        all_times = range(start, end + step, step)
    else:
        step = 60 * 60
        all_times = range(start, end + step, step)
    for t in all_times:
        cmd1 = configurations[KEY_TASK_OBJ].replace('"', '')
        cmd2 = configurations[KEY_PARA1].replace("${unix_timestamp}", str(t)).replace('"', "'")
        cmd = cmd1 + ' ' + cmd2
        print cmd
        a, b = commands.getstatusoutput(cmd)
        if a != SUCCESS:
            print b
            with open("fail.tasks", "a") as f:
                f.write(cmd + "\n")
        else:
            print b
            with open("success.tasks", "a") as f:
                f.write(cmd + "\n")


# python run_history.py task_id start_time end_time
if __name__ == "__main__":
    tasks = init()
    run(sys.argv[1:], tasks)
