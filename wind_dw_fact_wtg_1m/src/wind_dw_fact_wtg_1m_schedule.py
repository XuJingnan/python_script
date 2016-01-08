import commands
import datetime
import sys

SUCCESS = 0
ERROR_CHECK_PARAMETERS = 1
ERROR_RM_DEST_HDFS_DIR = 2
ERROR_RUN_STREAM_TASK = 3
ERROR_DROP_PARTITION = 4
ERROR_ADD_PARTITION = 5

IN_TABLE_NAME = "wind_ods_fact_wtg_1m_raw"
OUT_TABLE_NAME = "wind_dw_fact_wtg_1m"


def init(argv):
    if len(argv) != 5:
        exit(ERROR_CHECK_PARAMETERS)
    hadoop_home = argv[0]
    hdfs_root = argv[1]
    day = (datetime.datetime.fromtimestamp(int(argv[2])) - datetime.timedelta(days=1)).strftime("%Y%m%d")
    hive_db = argv[3]
    program_base = argv[4]
    return hadoop_home, hdfs_root, day, hive_db, program_base


def rm_dest_hdfs_dir(hdfs_root, day):
    rm_hdfs_dir_cmd = "hadoop fs -rmr /%s/bi/logs/wind/%s/hp_date=%s" % (hdfs_root, OUT_TABLE_NAME, day)
    print rm_hdfs_dir_cmd
    a, b = commands.getstatusoutput(rm_hdfs_dir_cmd)
    if a != SUCCESS:
        print "rm dest hdfs dir fail!"
        print b
    else:
        print "rm dest hdfs dir successfully!"


def run_stream_task(hadoop_home, hdfs_root, day, program_base):
    stream_cmd = "hadoop jar %s/share/hadoop/tools/lib/hadoop-streaming-2.6.0.jar -D mapreduce.job.reduces=0 " \
                 "-D mapreduce.job.name='%s_%s' " \
                 "-files %s/%s.py," \
                 "hdfs://titan/%s/bi/logs/conf/wind/%s/wind_dw_fact_wtg_1m_input_schema.conf " \
                 "-input /%s/bi/logs/wind/%s/hp_date=%s " \
                 "-output /%s/bi/logs/wind/%s/hp_date=%s " \
                 "-mapper 'python %s.py'" \
                 % (hadoop_home,
                    OUT_TABLE_NAME, day,
                    program_base, OUT_TABLE_NAME,
                    hdfs_root, OUT_TABLE_NAME,
                    hdfs_root, IN_TABLE_NAME, day,
                    hdfs_root, OUT_TABLE_NAME, day,
                    OUT_TABLE_NAME)
    print stream_cmd
    a, b = commands.getstatusoutput(stream_cmd)
    if a != SUCCESS:
        print "run steam task fail!"
        print b
        return ERROR_RUN_STREAM_TASK
    else:
        print "run steam task successful!"
        return SUCCESS


def add_partition(day, hive_db, hdfs_root):
    drop_partition_cmd = 'hive -e "use %s; alter table %s drop if exists partition (hp_date=\'%s\')"' % (
        hive_db, OUT_TABLE_NAME, day)
    print drop_partition_cmd
    a, b = commands.getstatusoutput(drop_partition_cmd)
    if a != SUCCESS:
        print "drop partition cmd error!"
        print b
        return ERROR_DROP_PARTITION
    else:
        print "drop partition cmd successful!"

    add_partition_cmd = 'hive -e "use %s; ' \
                        'alter table %s add partition (hp_date = \'%s\') ' \
                        'location \'/%s/bi/logs/wind/%s/hp_date=%s\'"' % (
                            hive_db, OUT_TABLE_NAME, day, hdfs_root, OUT_TABLE_NAME, day)
    print add_partition_cmd
    a, b = commands.getstatusoutput(add_partition_cmd)
    if a != SUCCESS:
        print "add partition cmd error!"
        print b
        return ERROR_ADD_PARTITION
    else:
        print "add partition cmd successful!"
        return SUCCESS


if __name__ == "__main__":
    hadoop_home, hdfs_root, day, hive_db, program_base = init(sys.argv[1:])
    rm_dest_hdfs_dir(hdfs_root, day)
    res = run_stream_task(hadoop_home, hdfs_root, day, program_base)
    if res != SUCCESS:
        exit(res)
    res = add_partition(day, hive_db, hdfs_root)
    if res == SUCCESS:
        print "task successful!"
    else:
        print "task fail!"
    exit(res)
