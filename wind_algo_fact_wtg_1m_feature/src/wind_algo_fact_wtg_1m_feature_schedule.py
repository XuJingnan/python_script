import commands
import datetime
import sys

SUCCESS = 0
ERROR_CHECK_PARAMETERS = 1
ERROR_RM_DEST_HDFS_DIR = 2
ERROR_RUN_STREAM_TASK = 3
ERROR_DROP_PARTITION = 4
ERROR_ADD_PARTITION = 5

IN_TABLE1_NAME = "wind_algo_fact_wtg_1m_feature_tmp"
IN_TABLE2_NAME = "wind_dw_dim_wtg_ideal_power_curve"
OUT_TABLE_NAME = "wind_algo_fact_wtg_1m_feature"
WAREHOUSE_HDFS_DIR = "/user/hive/warehouse"


def init(argv):
    if len(argv) != 4:
        exit(ERROR_CHECK_PARAMETERS)
    hadoop_home = argv[0]
    day = (datetime.datetime.fromtimestamp(int(argv[1])) - datetime.timedelta(days=1)).strftime("%Y%m%d")
    hive_db = argv[2]
    program_base = argv[3]
    return hadoop_home, day, hive_db, program_base


def rm_dest_hdfs_dir(hive_db, day):
    rm_hdfs_dir_cmd = "hadoop fs -rmr %s/%s.db/%s/hp_date=%s" % (WAREHOUSE_HDFS_DIR, hive_db, OUT_TABLE_NAME, day)
    print rm_hdfs_dir_cmd
    a, b = commands.getstatusoutput(rm_hdfs_dir_cmd)
    if a != SUCCESS:
        print "rm dest hdfs dir fail!"
        print b
    else:
        print "rm dest hdfs dir successfully!"


def run_stream_task(hadoop_home, day, program_base, hive_db):
    stream_cmd = "hadoop jar %s/share/hadoop/tools/lib/hadoop-streaming-2.6.0.jar " \
                 "-D mapreduce.job.name='%s_%s' " \
                 "-D stream.num.map.output.key.fields=2 " \
                 "-D num.key.fields.for.partition=1 " \
                 "-D mapred.reduce.tasks=10 " \
                 "-files %s/%s_map.py,%s/%s_reduce.py " \
 \
                 "-input %s/%s.db/%s/hp_date=%s," \
                 "%s/%s.db/%s/hp_ds=* " \
                 "-output %s/%s.db/%s/hp_date=%s " \
                 "-mapper 'python %s_map.py' " \
                 "-partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner " \
                 "-reducer 'python %s_reduce.py' " \
                 % (hadoop_home,
                    OUT_TABLE_NAME, day,
                    program_base, OUT_TABLE_NAME, program_base, OUT_TABLE_NAME,
                    WAREHOUSE_HDFS_DIR, hive_db, IN_TABLE1_NAME, day,
                    WAREHOUSE_HDFS_DIR, hive_db, IN_TABLE2_NAME,
                    WAREHOUSE_HDFS_DIR, hive_db, OUT_TABLE_NAME, day,
                    OUT_TABLE_NAME,
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


def add_partition(day, hive_db):
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

    add_partition_cmd = 'hive -e "use %s; alter table %s add partition (hp_date = \'%s\');" ' \
                        % (hive_db, OUT_TABLE_NAME, day)
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
    hadoop_home, day, hive_db, program_base = init(sys.argv[1:])
    rm_dest_hdfs_dir(hive_db, day)
    res = run_stream_task(hadoop_home, day, program_base, hive_db)
    if res != SUCCESS:
        exit(res)
    res = add_partition(day, hive_db)
    if res == SUCCESS:
        print "task successful!"
    else:
        print "task fail!"
    exit(res)
