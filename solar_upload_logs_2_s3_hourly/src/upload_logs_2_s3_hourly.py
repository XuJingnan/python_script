import datetime
import os.path
import subprocess
import sys

SUCCESS = 0
ERROR_CHECK_PARAMETERS = 1
ERROR_GET_LOGS = 2
ERROR_CHECK_S3_FILE = 3
ERROR_RM_S3_FILE = 4
ERROR_UPLOAD_FILE = 5
ERROR_RM_SERVER_FILE = 6
SUCCESS_DONE = 7


def exe_cmd(cmd):
    try:
        b = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)
        return 0, b
    except subprocess.CalledProcessError as e:
        return e.returncode, e.output


def parameters_check(argv):
    if len(argv) != 1:
        print("parameters format check:\terror!")
        return ERROR_CHECK_PARAMETERS
    return SUCCESS


def get_logs(day, hour):
    ls_cmd = '''dir *.%s.%s | find "log_"''' % (day, hour)
    a, b = exe_cmd(ls_cmd)
    if a == 0:
        return SUCCESS, b
    elif os.path.exists("%s.%s.done" % (day, hour)):
        return SUCCESS_DONE, b
    else:
        print("none logs for %s.%s" % (day, hour))
        return ERROR_GET_LOGS, None


def s3_file_check(dest_path):
    test_cmd = "aws s3 ls %s" % dest_path
    a, b = exe_cmd(test_cmd)
    if a == 0:
        print("s3 file check:\t%s exists." % dest_path)
        return ERROR_CHECK_S3_FILE
    else:
        return SUCCESS


def rm_s3_file(dest_path):
    rm_cmd = "aws s3 rm %s" % dest_path
    a, b = exe_cmd(rm_cmd)
    if a != 0:
        print("rm s3 file %s error!" % dest_path)
        print("error log\n%s" % b)
        return ERROR_RM_S3_FILE
    else:
        print("rm s3 file %s success!" % dest_path)
        return SUCCESS


def upload_file(file_name, dest_path):
    upload_cmd = "aws s3 cp %s %s" % (file_name, dest_path)
    a, b = exe_cmd(upload_cmd)
    if a == 0:
        print("upload file %s to %s success!" % (file_name, dest_path))
        return SUCCESS
    else:
        print("cmd:%s\n" % upload_cmd)
        print("return code:%s\n" % a)
        print("error log:\n%s" % b)
        print("upload file %s to %s fail!" % (file_name, dest_path))
        return ERROR_UPLOAD_FILE


def rm_server_file(file_name, day, hour):
    a, b = exe_cmd("rm %s" % file_name)
    if a != 0:
        print("rm server file %s error!" % file_name)
        print("error log\n%s" % b)
        return ERROR_RM_SERVER_FILE
    else:
        print("rm server file %s success!" % file_name)
    with open("%s.%s.done" % (day, hour), "a") as f:
        f.write("%s has uploaded!\n" % file_name)
    return SUCCESS


def upload_log_2_s3(file_name):
    log_type, site_id, day, hour = file_name.split(".")
    dest_path = "s3://ocean/Log/solar/%s/site=%s/yyyymmdd=%s/%s" % (log_type, site_id, day, file_name)
    res = s3_file_check(dest_path)
    if res != SUCCESS:
        res = rm_s3_file(dest_path)
        if res != SUCCESS:
            return res

    res = upload_file(file_name, dest_path)
    if res != SUCCESS:
        return res

    res = rm_server_file(file_name, day, hour)
    return res


def upload_logs_2_s3(argv):
    res = parameters_check(argv)
    if res != SUCCESS:
        exit(res)

    day, hour = (datetime.datetime.fromtimestamp(int(argv[0])) - datetime.timedelta(hours=9)).strftime(
        '%Y%m%d %H').split(' ')

    res, logs = get_logs(day, hour)
    if res == SUCCESS_DONE:
        print("has done %s.%s before" % (day, hour))
        exit(SUCCESS)
    elif res == ERROR_GET_LOGS:
        exit(res)

    logs = [log[log.find('log_'):] for log in logs.decode('gb2312').split('\r\n') if log != '']
    for log in logs:
        res = upload_log_2_s3(log)
        if res != SUCCESS:
            exit(res)
    return SUCCESS


# cmd example:   python upload_logs_2_s3_hourly.py 1449536400
if __name__ == "__main__":
    res = upload_logs_2_s3(sys.argv[1:])
    if res == SUCCESS:
        print("upload logs 2 s3 successfully")
    else:
        print("upload logs 2 s3 fail")
    exit(res)
