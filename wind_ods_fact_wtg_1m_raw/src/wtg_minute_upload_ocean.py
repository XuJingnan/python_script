import commands
import datetime
import os
import sys

SUCCESS = 0
ERROR_CHECK_PARAMETERS = 1
ERROR_GET_S3_FILE = 2
ERROR_GET_LOCAL_PATHS = 3
ERROR_EXTRACT_FILE = 4
ERROR_WRONG_DATA_CONTENT = 5
ERROR_WTG_ID_NOT_EXIST = 6
ERROR_CLEAN_KEY_NOT_EXIST = 7
ERROR_CREATE_HDFS_DIR = 8
ERROR_TEST_HDFS_FILE = 9
ERROR_RM_HDFS_FILE = 10
ERROR_PUT_HDFS_FILE = 11
ERROR_DROP_PARTITION = 12
ERROR_ADD_PARTITION = 13
ERROR_RM_LOCAL_FILE = 14

KEY_SITE_ID = "site_id"
KEY_WTG_ID = "wtg_id"
KEY_LOCAL_DIR = "local_dir"
KEY_FILE_NAME = "file_name"

S3_ROOT_PATH = "s3://envwfdata/Daily_export"
DATA_DIR = "result/data"
LOG_DIR = "result/log"
OUTPUT_DIR = "result/output"

TABLE_NAME = "wind_ods_fact_wtg_1m_raw"
RESULT_FILE_INDEX = 0

MAX_RECORD_NUMBER = 100000
records = []


def parameters_check(argv):
    if len(argv) != 3:
        print "parameters format check:\terror!"
        return ERROR_CHECK_PARAMETERS
    return SUCCESS


def init(day):
    id_map = {}
    with open("wtg_mdm.csv") as f:
        for line in f:
            wtg_id, alias, site_id = line.strip().split(",")
            if alias == "NULL":
                continue
            tmp = alias.split(".")
            alias = "/".join([S3_ROOT_PATH, tmp[0], tmp[2], day, "minute"])
            local_dir = "/".join([DATA_DIR, tmp[0], tmp[2], day, "minute"])
            file_name = "minute_%s.csv" % day
            id_map[alias] = {KEY_SITE_ID: site_id, KEY_WTG_ID: wtg_id, KEY_LOCAL_DIR: local_dir,
                             KEY_FILE_NAME: file_name}
    global LOG_DIR
    LOG_DIR += "/%s" % day
    rm_cmd = "rm -rf %s %s" % (DATA_DIR, OUTPUT_DIR)
    commands.getstatusoutput(rm_cmd)
    mkdir_cmd = "mkdir -p %s %s %s" % (LOG_DIR, DATA_DIR, OUTPUT_DIR)
    commands.getstatusoutput(mkdir_cmd)
    return id_map


def get_s3_file(id_map):
    success_list = []
    fail_list = []
    for s3_path in sorted(id_map.keys()):
        value = id_map.get(s3_path)
        local_dir = value.get(KEY_LOCAL_DIR)
        file_name = value.get(KEY_FILE_NAME)
        get_cmd = "mkdir -p %s && cd %s && " \
                  "aws s3 cp %s/%s.7z . && cd -" \
                  % (local_dir, local_dir, s3_path, file_name)
        print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), get_cmd
        a, b = commands.getstatusoutput(get_cmd)
        if a != SUCCESS:
            fail_list.append("%s/%s.7z" % (s3_path, file_name))
        else:
            success_list.append("%s/%s.7z" % (s3_path, file_name))
    with open("%s/get_s3_file.success" % LOG_DIR, "w") as s_file, open("%s/get_s3_file.fail" % LOG_DIR, "w") as f_file:
        for s in success_list:
            s_file.write(s + "\n")
        for f in fail_list:
            f_file.write(f + "\n")
    return SUCCESS


def write_error_log(error_file, message):
    with open(error_file) as f:
        f.write(message + "\n")


def create_hdfs_dir(hdfs_dir):
    create_cmd = "hadoop fs -mkdir -p %s" % hdfs_dir
    print create_cmd
    a, b = commands.getstatusoutput(create_cmd)
    if a != SUCCESS:
        print "create hdfs dir error!"
        return ERROR_CREATE_HDFS_DIR
    else:
        return SUCCESS


def rm_hdfs_dir(hdfs_dir):
    rm_cmd = "hadoop fs -rmr %s" % hdfs_dir
    print rm_cmd
    a, b = commands.getstatusoutput(rm_cmd)
    if a == 0:
        return SUCCESS
    else:
        print "rm hdfs exist file fail!"
        return SUCCESS


def put_hdfs_file(hdfs_dir, file_name):
    put_cmd = "hadoop fs -put %s %s/%s" % (file_name, hdfs_dir, file_name.split("/")[-1])
    print put_cmd
    a, b = commands.getstatusoutput(put_cmd)
    if a != SUCCESS:
        print "put file to hdfs error!"
        return ERROR_PUT_HDFS_FILE
    else:
        return SUCCESS


def add_partition(hdfs_dir, hive_db):
    tmp = hdfs_dir.split("/")
    table_name = tmp[-2]
    day = tmp[-1].split("=")[-1]
    drop_partition_cmd = 'hive -e "use %s; alter table %s drop if exists partition (hp_date=\'%s\')"' % (
        hive_db, table_name, day)
    print drop_partition_cmd
    a, b = commands.getstatusoutput(drop_partition_cmd)
    if a != SUCCESS:
        print "drop partition cmd error!"
        return ERROR_DROP_PARTITION

    add_partition_cmd = 'hive -e "use %s; alter table %s add partition (hp_date = \'%s\') location \'hdfs://titan%s/\'"' % (
        hive_db, table_name, day, hdfs_dir)
    print add_partition_cmd
    a, b = commands.getstatusoutput(add_partition_cmd)
    if a != SUCCESS:
        print "add partition cmd error!"
        return ERROR_ADD_PARTITION
    return SUCCESS


def rm_local_file():
    rm_cmd = "rm -r %s %s" % (OUTPUT_DIR, DATA_DIR)
    a, b = commands.getstatusoutput(rm_cmd)
    if a != SUCCESS:
        print "rm local file error!"
        return ERROR_RM_LOCAL_FILE
    else:
        return SUCCESS


def put_data_2_ocean(hdfs_dir, hive_db):
    return_code = rm_hdfs_dir(hdfs_dir)
    if return_code != SUCCESS:
        return return_code

    return_code = create_hdfs_dir(hdfs_dir)
    if return_code != SUCCESS:
        return return_code

    files = [OUTPUT_DIR + "/part.%s" % index for index in range(RESULT_FILE_INDEX)]
    for file_name in files:
        return_code = put_hdfs_file(hdfs_dir, file_name)
        if return_code != SUCCESS:
            return return_code

    return_code = add_partition(hdfs_dir, hive_db)
    if return_code != SUCCESS:
        return return_code

    return_code = rm_local_file()
    return return_code


def extract_file(path, file_name):
    extract_cmd = "7za x %s/%s.7z -y -o%s" % (path, file_name, path)
    a, b = commands.getstatusoutput(extract_cmd)
    return a


def output(record):
    li = []
    for key in record:
        li.append(":".join([key, record[key]]))
    records.append(",".join(li))
    if len(records) >= MAX_RECORD_NUMBER:
        output_batch()


def output_batch():
    global RESULT_FILE_INDEX
    with open("%s/part.%s" % (OUTPUT_DIR, RESULT_FILE_INDEX), "a") as f:
        for line in records:
            f.write(line + "\n")
        records[:] = []
        RESULT_FILE_INDEX += 1


# insert wtg_id, site_id for each wtg
def single_wtg_process(values, day):
    path = values.get(KEY_LOCAL_DIR)
    file_name = values.get(KEY_FILE_NAME)
    print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "start processing", "%s/%s.7z" % (path, file_name)
    ids = {KEY_WTG_ID: values.get(KEY_WTG_ID), KEY_SITE_ID: values.get(KEY_SITE_ID)}
    return_code = extract_file(path, file_name)
    if return_code != SUCCESS:
        write_error_log("%s/extract_file_error" % LOG_DIR, "%s.7z" % file_name)
        return SUCCESS
    if not os.path.exists(path + "/" + file_name):
        return SUCCESS
    with open(path + "/" + file_name) as f:
        header = f.readline().strip().split(",")
        for line in f:
            fields = line.strip().split(",")
            if len(header) != len(fields):
                write_error_log("%s/error_wrong_data_content" % LOG_DIR, file_name)
                return SUCCESS
            record = dict(zip(header, fields))
            # add site_id and wtg_id
            record[KEY_SITE_ID] = ids[KEY_SITE_ID]
            record[KEY_WTG_ID] = ids[KEY_WTG_ID]
            output(record)
    return SUCCESS


def upload_logs_2_ocean(argv):
    return_code = parameters_check(argv)
    if return_code != SUCCESS:
        exit(return_code)

    day = (datetime.datetime.fromtimestamp(int(argv[0])) - datetime.timedelta(days=1)).strftime('%Y%m%d')
    hdfs_dir = "%s/wind/%s/hp_date=%s" % (argv[1], TABLE_NAME, day)
    hive_db = argv[2]
    id_map = init(day)

    return_code = get_s3_file(id_map)
    if return_code != SUCCESS:
        exit(return_code)

    for v in sorted(id_map.values(), key=lambda item: item.get(KEY_LOCAL_DIR)):
        ls_cmd = "ls %s/%s.7z" % (v.get(KEY_LOCAL_DIR), v.get(KEY_FILE_NAME))
        a, b = commands.getstatusoutput(ls_cmd)
        if a != SUCCESS:
            continue
        return_code = single_wtg_process(v, day)
        if return_code != SUCCESS:
            return return_code
    output_batch()
    return_code = put_data_2_ocean(hdfs_dir, hive_db)
    return return_code


# cmd example:   python wtg_minute_upload_ocean.py 1450886400 $HDFS_LOG_ROOT $HIVE_DB
# two problems not resolved:
# 1. mdm data update if new wtg added
# 2. data collected from s3 maybe not complete
# only add wtg_id, site_id for each wtg
if __name__ == "__main__":
    res = upload_logs_2_ocean(sys.argv[1:])
    if res == SUCCESS:
        print "finish successfully!"
