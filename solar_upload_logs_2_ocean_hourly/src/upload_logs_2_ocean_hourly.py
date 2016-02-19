import Queue
import commands
import datetime
import sys
import threading

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
    if len(argv) != 2:
        print "parameters format check:\terror!"
        return ERROR_CHECK_PARAMETERS
    return SUCCESS


def get_s3_file(day, hour):
    mkdir_cmd = "mkdir -p input/{day}.{hour}".format(day=day, hour=hour)
    commands.getstatusoutput(mkdir_cmd)
    get_cmd = "aws s3 cp s3://ocean/Log/solar input/{day}.{hour}/ " \
              "--recursive --exclude '*' --include '*yyyymmdd={day}*'".format(day=day, hour=hour)
    print get_cmd
    a, b = commands.getstatusoutput(get_cmd)
    if a != 0:
        print "get s3 file error!"
        return ERROR_GET_S3_FILE
    else:
        print "get s3 file success!"
        return SUCCESS


def get_local_paths(day, hour):
    get_cmd = "ls -R input/{day}.{hour} | grep yyyymmdd={day}:".format(day=day, hour=hour)
    print get_cmd
    a, b = commands.getstatusoutput(get_cmd)
    if a != 0:
        print "get local paths error!"
        return ERROR_GET_LOCAL_PATHS, None
    else:
        print "get local paths success!"
        return SUCCESS, b


def create_hdfs_dir(hdfs_dest_site_dir):
    create_cmd = "hadoop fs -mkdir -p /%s" % hdfs_dest_site_dir
    print create_cmd
    a, b = commands.getstatusoutput(create_cmd)
    if a != 0:
        print "log:\n%s" % b
        return ERROR_CREATE_HDFS_DIR, "create hdfs dir error!"
    else:
        return SUCCESS, None


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
    commands.getstatusoutput(rm_cmd)


def put_hdfs_file(path, hdfs_dest_site_dir, day_dir):
    put_cmd = "hadoop fs -put %s /%s/%s" % (path, hdfs_dest_site_dir, day_dir)
    print put_cmd
    a, b = commands.getstatusoutput(put_cmd)
    if a != SUCCESS:
        print "log:\n%s" % b
        return ERROR_PUT_HDFS_FILE, "put file to hdfs error!"
    else:
        return SUCCESS, "put file to hdfs success!"


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
    add_partition_cmd = 'hive -e "use %s; alter table %s add if not exists partition (site = \'%s\', yyyymmdd = \'%s\')"' % (
        hive_db, table_name, site_id, day)
    print add_partition_cmd
    a, b = commands.getstatusoutput(add_partition_cmd)
    if a != 0:
        print "add partition cmd error!"
        return ERROR_ADD_PARTITION
    return SUCCESS


def rm_local_file(path):
    rm_cmd = "rm -r %s" % path
    a, b = commands.getstatusoutput(rm_cmd)
    if a != 0:
        print "log:\n%s" % b
        return ERROR_RM_LOCAL_FILE, "rm local file error!"
    else:
        return SUCCESS, "rm local file success!"


def upload_log_2_ocean(path, hdfs_root):
    input_dir, day_hour, log_type, site_id, day = path.split("/")
    hdfs_dest_site_dir = "/".join([hdfs_root, "bi/logs/solar", log_type, site_id])
    day_dir = day
    rm_hdfs_file(hdfs_dest_site_dir, day_dir)
    res, msg = create_hdfs_dir(hdfs_dest_site_dir)
    if res != SUCCESS:
        q.put((path, res, msg))
        return
    res, msg = put_hdfs_file(path, hdfs_dest_site_dir, day_dir)
    if res != SUCCESS:
        q.put((path, res, msg))
    res, msg = rm_local_file(path)
    q.put((path, res, msg))


q = Queue.Queue()


def batch_process(threads):
    for t in threads:
        t.start()
    for t in threads:
        t.join()


def clean(day, hour):
    rm_cmd = "rm -rf input/{day}.{hour}".format(day=day, hour=hour)
    commands.getstatusoutput(rm_cmd)


def upload_logs_2_ocean(argv):
    res = parameters_check(argv)
    if res != SUCCESS:
        exit(res)

    day, hour = (datetime.datetime.fromtimestamp(int(argv[0])) - datetime.timedelta(hours=9)).strftime(
        '%Y%m%d %H').split(" ")
    hdfs_root = argv[1]

    res = get_s3_file(day, hour)
    if res != SUCCESS:
        exit(res)

    res, local_paths = get_local_paths(day, hour)
    if res != SUCCESS:
        exit(res)
    local_paths = [p[:-1] for p in local_paths.split("\n")]

    threads = []
    cnt = 0
    for path in local_paths:
        t = threading.Thread(target=upload_log_2_ocean, name='thread-' + path, args=(path, hdfs_root,))
        threads.append(t)
        cnt += 1
        if cnt % 20 == 0:
            batch_process(threads)
            threads = []
    batch_process(threads)

    clean(day, hour)
    result = list()
    while not q.empty():
        result.append(q.get())
    return_code = 0
    for item in result:
        if item[1] != 0:
            return_code += item[1]
            print "{path} occurs {error} error".format(path=item[0], error=item[2])
    if return_code == 0:
        print "{day}.{hour} task success!".format(day=day, hour=hour)
    exit(return_code)


# cmd example:   python upload_logs_2_ocean_hourly.py 1452571200 prod
if __name__ == "__main__":
    upload_logs_2_ocean(sys.argv[1:])
