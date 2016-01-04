import commands
import datetime
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

SITE_ID_KEY = "site_id"
WTG_ID_KEY = "wtg_id"
LOCAL_DIR_KEY = "local_dir"
FILE_NAME_KEY = "file_name"

S3_ROOT_PATH = "s3://envwfdata/Daily_export"
LOCAL_ROOT_DIR = "data"
NULL = "NULL"

RESULT_FILE = "wtg_minute_raw.txt"

MAX_RECORD_NUMBER = 10000
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
            local_dir = "/".join([LOCAL_ROOT_DIR, tmp[0], tmp[2], day, "minute"])
            file_name = "minute_%s.csv" % day
            id_map[alias] = {SITE_ID_KEY: site_id, WTG_ID_KEY: wtg_id, LOCAL_DIR_KEY: local_dir,
                             FILE_NAME_KEY: file_name}

    with open("schema.conf") as f:
        schema = f.readline().strip().split(",")

    rm_cmd = "rm -rf %s" % RESULT_FILE
    mkdir_cmd = "mkdir log"
    commands.getstatusoutput(rm_cmd)
    commands.getstatusoutput(mkdir_cmd)

    return id_map, schema


def get_s3_file(day, id_map):
    success_list = []
    fail_list = []
    for s3_path in sorted(id_map.keys()):
        value = id_map.get(s3_path)
        local_dir = value.get(LOCAL_DIR_KEY)
        file_name = value.get(FILE_NAME_KEY)
        get_cmd = "mkdir -p %s && cd %s && " \
                  "aws s3 cp %s/%s.7z . && cd -" \
                  % (local_dir, local_dir, s3_path, file_name)
        print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), get_cmd
        a, b = commands.getstatusoutput(get_cmd)
        if a != SUCCESS:
            fail_list.append("%s/%s.7z" % (s3_path, file_name))
        else:
            success_list.append("%s/%s.7z" % (s3_path, file_name))
    with open("log/%s.success" % day, "w") as s_file, open("log/%s.fail" % day, "w") as f_file:
        for s in success_list:
            s_file.write(s + "\n")
        for f in fail_list:
            f_file.write(f + "\n")
    return SUCCESS


def create_hdfs_dir(hdfs_dir):
    test_cmd = "hadoop fs -test -d %s" % hdfs_dir
    print test_cmd
    a, b = commands.getstatusoutput(test_cmd)
    if a == 0:
        return SUCCESS

    create_cmd = "hadoop fs -mkdir -p %s" % hdfs_dir
    print create_cmd
    a, b = commands.getstatusoutput(create_cmd)
    if a != SUCCESS:
        print "create hdfs dir error!"
        return ERROR_CREATE_HDFS_DIR
    else:
        return SUCCESS


def test_hdfs_file(hdfs_dir, file_name):
    test_cmd = "hadoop fs -test -f %s/%s" % (hdfs_dir, file_name)
    print test_cmd
    a, b = commands.getstatusoutput(test_cmd)
    if a == 0:
        print "hdfs file exists!"
        return ERROR_TEST_HDFS_FILE
    else:
        return SUCCESS


def rm_hdfs_file(hdfs_dir, file_name):
    rm_cmd = "hadoop fs -rmr %s/%s" % (hdfs_dir, file_name)
    print rm_cmd
    a, b = commands.getstatusoutput(rm_cmd)
    if a == 0:
        return SUCCESS
    else:
        print "rm hdfs exist file fail!"
        return ERROR_RM_HDFS_FILE


def put_hdfs_file(hdfs_dir, file_name):
    put_cmd = "hadoop fs -put %s %s/%s" % (file_name, hdfs_dir, file_name)
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


def rm_local_file(file_name):
    rm_cmd = "rm -r %s" % file_name
    a, b = commands.getstatusoutput(rm_cmd)
    if a != SUCCESS:
        print "rm local file error!"
        return ERROR_RM_LOCAL_FILE
    else:
        return SUCCESS


def put_data_2_ocean(hdfs_dir, file_name, hive_db):
    return_code = create_hdfs_dir(hdfs_dir)
    if return_code != SUCCESS:
        return return_code

    return_code = test_hdfs_file(hdfs_dir, file_name)
    if return_code != SUCCESS:
        return_code = rm_hdfs_file(hdfs_dir, file_name)
        if return_code != SUCCESS:
            return return_code

    return_code = put_hdfs_file(hdfs_dir, file_name)
    if return_code != SUCCESS:
        return return_code

    return_code = add_partition(hdfs_dir, hive_db)
    if return_code != SUCCESS:
        return return_code

    return_code = rm_local_file(file_name)
    return return_code


def get_ids(id_map, site_name, wtg_name, day):
    key = "/".join([S3_ROOT_PATH, site_name, wtg_name, day, "minute"])
    return id_map.get(key)


def extract_file(path, file_name):
    extract_cmd = "7za x %s/%s.7z -y -o%s" % (path, file_name, path)
    a, b = commands.getstatusoutput(extract_cmd)
    return a


#
# def data_clean(record, clean):
#     for k in clean:
#         v = clean.get(k)
#         real_value = record.get(k)
#         if real_value is None:
#             exit(ERROR_CLEAN_KEY_NOT_EXIST)
#         if real_value == NULL:
#             continue
#         if float(real_value) > v.get(HIGH) * 2 or float(real_value) < v.get(LOW):
#             record[k] = NULL


def output(record, schema):
    global records

    li = []
    # schema mapping
    for field in schema:
        if record.has_key(field):
            li.append(record[field])
        else:
            li.append(NULL)
    records.append(",".join(li))

    if len(records) >= MAX_RECORD_NUMBER:
        # merge data
        output_batch()


def output_batch():
    with open(RESULT_FILE, "a") as f:
        for line in records:
            f.write(line + "\n")
        records[:] = []


def finalize():
    output_batch()
    rm_result_file_cmd = "rm -rf data"
    commands.getstatusoutput(rm_result_file_cmd)


def single_wtg_process(values, schema):
    path = values.get(LOCAL_DIR_KEY)
    file_name = values.get(FILE_NAME_KEY)
    print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "start processing", "%s/%s.7z" % (path, file_name)
    ids = {WTG_ID_KEY: values.get(WTG_ID_KEY), SITE_ID_KEY: values.get(SITE_ID_KEY)}
    return_code = extract_file(path, file_name)
    if return_code != SUCCESS:
        print 'extract file error: %s.7z' % file_name
        return ERROR_EXTRACT_FILE

    with open(path + "/" + file_name) as f:
        header = f.readline().strip().split(",")
        for line in f:
            fields = line.strip().split(",")
            if len(header) != len(fields):
                exit(ERROR_WRONG_DATA_CONTENT)
            record = dict(zip(header, fields))
            # add site_id and wtg_id
            record[SITE_ID_KEY] = ids[SITE_ID_KEY]
            record[WTG_ID_KEY] = ids[WTG_ID_KEY]
            output(record, schema)
    return SUCCESS


def upload_logs_2_ocean(argv):
    return_code = parameters_check(argv)
    if return_code != SUCCESS:
        exit(return_code)

    day = (datetime.datetime.fromtimestamp(int(argv[0])) - datetime.timedelta(days=1)).strftime('%Y%m%d')
    hdfs_dir = "%s/wind/wtg_minute_raw/hp_date=%s" % (argv[1], day)
    hive_db = argv[2]
    id_map, schema = init(day)

    return_code = get_s3_file(day, id_map)
    if return_code != SUCCESS:
        exit(return_code)

    for v in sorted(id_map.values(), key=lambda item: item.get(LOCAL_DIR_KEY)):
        ls_cmd = "ls %s/%s.7z" % (v.get(LOCAL_DIR_KEY), v.get(FILE_NAME_KEY))
        a, b = commands.getstatusoutput(ls_cmd)
        if a != SUCCESS:
            continue
        return_code = single_wtg_process(v, schema)
        if return_code != SUCCESS:
            return return_code
    finalize()

    return_code = put_data_2_ocean(hdfs_dir, RESULT_FILE, hive_db)
    return return_code


# cmd example:   python wtg_minute_upload_ocean.py 1450886400 $HDFS_LOG_ROOT $HIVE_DB
# two problems not resolved:
# 1. mdm data update if new wtg added
# 2. data collected from s3 maybe not complete
if __name__ == "__main__":
    res = upload_logs_2_ocean(sys.argv[1:])
    if res == SUCCESS:
        print "finish successfully!"
