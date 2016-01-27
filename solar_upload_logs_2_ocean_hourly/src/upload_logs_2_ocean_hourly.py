import commands
import datetime
import sys

SUCCESS = 0
ERROR_CHECK_PARAMETERS = 1
ERROR_GET_S3_FILE = 2
ERROR_GET_LOCAL_PATHS = 3
ERROR_CREATE_HDFS_DIR = 4
ERROR_TEST_HDFS_FILE = 5
ERROR_RM_HDFS_FILE = 6
ERROR_PUT_HDFS_FILE = 7
ERROR_DROP_PARTITION = 8
ERROR_ADD_PARTITION = 9
ERROR_RM_LOCAL_FILE = 10


def parameters_check(argv):
    if len(argv) != 3:
        print "parameters format check:\terror!"
        return ERROR_CHECK_PARAMETERS
    return SUCCESS


def get_s3_file(day):
    get_cmd = "aws s3 cp s3://ocean/Log/solar . --recursive --exclude '*' --include '*yyyymmdd=%s*'" % (day)
    print get_cmd
    a, b = commands.getstatusoutput(get_cmd)
    if a != 0:
        print "get s3 file error!"
        return ERROR_GET_S3_FILE
    else:
        print "get s3 file success!"
        return SUCCESS


def get_local_paths(day):
    get_cmd = "ls -R | grep yyyymmdd=%s:" % day
    a, b = commands.getstatusoutput(get_cmd)
    if a != 0:
        print "get local paths error!"
        return ERROR_GET_LOCAL_PATHS, None
    else:
        print "get local paths success!"
        return SUCCESS, b


def create_hdfs_dir(hdfs_dest_site_dir):
    test_cmd = "hadoop fs -test -d /%s" % hdfs_dest_site_dir
    print test_cmd
    a, b = commands.getstatusoutput(test_cmd)
    if a == 0:
        return SUCCESS

    create_cmd = "hadoop fs -mkdir -p /%s" % hdfs_dest_site_dir
    print create_cmd
    a, b = commands.getstatusoutput(create_cmd)
    if a != 0:
        print "create hdfs dir error!"
        print "log:\n%s" % b
        return ERROR_CREATE_HDFS_DIR
    else:
        print "create hdfs dir success!"
        return SUCCESS


def test_hdfs_file(hdfs_dest_site_dir, day_dir):
    test_cmd = "hadoop fs -test -d /%s/%s" % (hdfs_dest_site_dir, day_dir)
    print test_cmd
    a, b = commands.getstatusoutput(test_cmd)
    if a == 0:
        print "hdfs file exists!"
        return ERROR_TEST_HDFS_FILE
    else:
        return SUCCESS


def rm_hdfs_file(hdfs_dest_site_dir, day_dir):
    rm_cmd = "hadoop fs -rmr /%s/%s" % (hdfs_dest_site_dir, day_dir)
    print rm_cmd
    a, b = commands.getstatusoutput(rm_cmd)
    if a == 0:
        print "rm hdfs exist file success!"
        return SUCCESS
    else:
        print "rm hdfs exist file fail!"
        print "log:\n%s" % b
        return ERROR_RM_HDFS_FILE


def put_hdfs_file(path, hdfs_dest_site_dir, day_dir):
    put_cmd = "hadoop fs -put %s /%s/%s" % (path, hdfs_dest_site_dir, day_dir)
    print put_cmd
    a, b = commands.getstatusoutput(put_cmd)
    if a != 0:
        print "put file to hdfs error!"
        print "log:\n%s" % b
        return ERROR_PUT_HDFS_FILE
    else:
        print "put file to hdfs success!"
        return SUCCESS


def add_partition(hdfs_dest_site_dir, day_dir, hive_db):
    tmp = hdfs_dest_site_dir.split("/")
    table_name = tmp[-2]
    site_info = tmp[-1]
    day_info = day_dir
    site_id = site_info.split("=")[1]
    day = day_info.split("=")[1]
    drop_partition_cmd = 'hive -e "use %s; alter table %s drop if exists partition (site=\'%s\', yyyymmdd=\'%s\')"' % (
        hive_db, table_name, site_id, day)
    print drop_partition_cmd
    a, b = commands.getstatusoutput(drop_partition_cmd)
    if a != 0:
        print "drop partition cmd error!"
        return ERROR_DROP_PARTITION
    add_partition_cmd = 'hive -e "use %s; alter table %s add partition (site = \'%s\', yyyymmdd = \'%s\')"' % (
        hive_db, table_name, site_id, day)
    print add_partition_cmd
    a, b = commands.getstatusoutput(add_partition_cmd)
    if a != 0:
        print "add partition cmd error!"
        return ERROR_ADD_PARTITION
    return SUCCESS


def rm_local_file():
    rm_cmd = "rm -r log*"
    print rm_cmd
    a, b = commands.getstatusoutput(rm_cmd)
    if a != 0:
        print "rm local file error!"
        print "log:\n%s" % b
        return ERROR_RM_LOCAL_FILE
    else:
        print "rm local file success!"
        return SUCCESS


def upload_log_2_ocean(path, hive_db, hdfs_root):
    log_type, site_id, day = path.split("/")
    hdfs_dest_site_dir = "/".join([hdfs_root, "bi/logs/solar", log_type, site_id])
    day_dir = day

    res = create_hdfs_dir(hdfs_dest_site_dir)
    if res != SUCCESS:
        return res

    res = test_hdfs_file(hdfs_dest_site_dir, day_dir)
    if res != SUCCESS:
        res = rm_hdfs_file(hdfs_dest_site_dir, day_dir)
        if res != SUCCESS:
            return res

    res = put_hdfs_file(path, hdfs_dest_site_dir, day_dir)
    if res != SUCCESS:
        return res

    res = add_partition(hdfs_dest_site_dir, day_dir, hive_db)
    return res


def upload_logs_2_ocean(argv):
    res = parameters_check(argv)
    if res != SUCCESS:
        exit(res)

    day = (datetime.datetime.fromtimestamp(int(argv[0])) - datetime.timedelta(hours=9)).strftime('%Y%m%d')
    hive_db = argv[1]
    hdfs_root = argv[2]

    res = get_s3_file(day)
    if res != SUCCESS:
        exit(res)

    res, local_paths = get_local_paths(day)
    if res != SUCCESS:
        exit(res)
    local_paths = [p[2:-1] for p in local_paths.split("\n")]

    for path in local_paths:
        res = upload_log_2_ocean(path, hive_db, hdfs_root)
        if res != SUCCESS:
            return res

    res = rm_local_file()
    exit(res)


# cmd example:   python upload_logs_2_ocean_hourly.py 1452571200 bi_dev dev
if __name__ == "__main__":
    upload_logs_2_ocean(sys.argv[1:])
