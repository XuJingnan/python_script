import Queue
import commands
import datetime
import sys
import threading

SUCCESS = 0
ERROR_CHECK_PARAMETERS = 1
ERROR_GET_LOGS = 2
ERROR_CHECK_S3_FILE = 3
ERROR_RM_S3_FILE = 4
ERROR_UPLOAD_FILE = 5
ERROR_RM_SERVER_FILE = 6

s3_path_prefix = 's3://ocean/Log/solar'


def exe_cmd(cmd):
    a, b = commands.getstatusoutput(cmd)
    return a, b


def get_logs(day):
    logs = []

    yesterday = (datetime.datetime.strptime(day, "%Y%m%d") + datetime.timedelta(days=-1)).strftime('%Y%m%d')
    the_day_before_yesterday = (datetime.datetime.strptime(day, "%Y%m%d") + datetime.timedelta(days=-2)).strftime(
        '%Y%m%d')

    ls_cmd = 'ls -l log*{day}*'.format(day=day)
    a, b = exe_cmd(ls_cmd)
    if a == 0:
        for log in b.split('\n'):
            logs.append(log.split(' ')[-1])
    else:
        print("none logs for {day}".format(day=day))

    ls_cmd = 'ls -l log*{day}*'.format(day=yesterday)
    a, b = exe_cmd(ls_cmd)
    if a == 0:
        for log in b.split('\n'):
            logs.append(log.split(' ')[-1])
    else:
        print("none logs for {day}".format(day=yesterday))

    ls_cmd = 'ls -l log*{day}*'.format(day=the_day_before_yesterday)
    a, b = exe_cmd(ls_cmd)
    if a == 0:
        for log in b.split('\n'):
            logs.append(log.split(' ')[-1])
    else:
        print("none logs for {day}".format(day=the_day_before_yesterday))

    logs = [log for log in logs if log != '']
    if len(logs) > 0:
        return SUCCESS, logs
    else:
        return ERROR_GET_LOGS, None


def upload_file(file_name, dest_path):
    upload_cmd = "aws s3 cp %s %s" % (file_name, dest_path)
    a, b = exe_cmd(upload_cmd)
    if a == 0:
        return SUCCESS, None
    else:
        return ERROR_UPLOAD_FILE, "upload file {file} error: {msg}".format(file=file_name, msg=b)


def rm_server_file(file_name, day, hour):
    a, b = exe_cmd("rm -rf %s" % file_name)
    if a != 0:
        return ERROR_RM_SERVER_FILE, "rm server file {file} error.".format(file=file_name)
    with open("%s.%s.done" % (day, hour), "a") as f:
        f.write("%s: %s has uploaded!\n" % (str(datetime.datetime.now()), file_name))
    the_day_last_month = (datetime.datetime.strptime(day, "%Y%m%d") + datetime.timedelta(days=-31)).strftime('%Y%m%d')
    rm_cmd = "rm {day}.{hour}.done".format(day=the_day_last_month, hour=hour)
    exe_cmd(rm_cmd)
    return SUCCESS, None


def upload_log_2_s3(file_name):
    log_type, site_id, day, hour = file_name.split(".")
    dest_path = "%s/%s/site=%s/yyyymmdd=%s/%s" % (s3_path_prefix, log_type, site_id, day, file_name)

    res, msg = upload_file(file_name, dest_path)
    if res != SUCCESS:
        q.put((file_name, res, msg))
        return

    res, msg = rm_server_file(file_name, day, hour)
    q.put((file_name, res, msg))


q = Queue.Queue()


def batch_process(threads):
    for t in threads:
        t.start()
    for t in threads:
        t.join()


def upload_logs_2_s3(argv):
    day, hour = (datetime.datetime.fromtimestamp(int(argv[0])) - datetime.timedelta(hours=9)).strftime(
        '%Y%m%d %H').split(' ')

    res, logs = get_logs(day)
    if res != SUCCESS:
        exit(SUCCESS)
    threads = []
    cnt = 0
    for log in logs:
        t = threading.Thread(target=upload_log_2_s3, name='thread-' + log, args=(log,))
        threads.append(t)
        cnt += 1
        if cnt % 10 == 0:
            batch_process(threads)
            threads = []
    batch_process(threads)

    result = list()
    while not q.empty():
        result.append(q.get())
    return_code = 0
    for item in result:
        if item[1] != 0:
            return_code += item[1]
            print("{path} occurs {error} error".format(path=item[0], error=item[2]))
    if return_code == 0:
        print("{day}.{hour} task success!".format(day=day, hour=hour))
    exit(return_code)


# cmd example:   python upload_logs_2_s3_hourly.py 1449536400
if __name__ == "__main__":
    upload_logs_2_s3(sys.argv[1:])
