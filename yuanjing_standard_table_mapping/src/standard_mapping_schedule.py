import commands
import datetime
import sys

import time

SUCCESS = 0
ERROR_CHECK_PARAMETERS = 1
ERROR_RM_DEST_HDFS_DIR = 2
ERROR_RUN_STREAM_TASK = 3
ERROR_DROP_PARTITION = 4
ERROR_ADD_PARTITION = 5

STREAMING_JAR = '/opt/cloudera/parcels/CDH/jars/hadoop-streaming-2.6.0-mr1-cdh5.4.8.jar'

JOB_BASE = '/var/lib/hadoop-hdfs/scheduler/standard-table-mapping'
MAPPER_SCRIPT = 'standard_mapping.py'
MAPPING_FILE = 'desc-alias-mapping.csv'
STANDARD_FILE = 'standard-table.csv'
WTG_MAPPING_FILE = 'wtg-master-mapping.csv'

HDFS_INPUT_PREFIX = '/merged/nrt'
HDFS_OUTPUT_PREFIX = '/user/hive/warehouse/bi.db/wind_ods_fact_wtg_1s_raw'
TABLE = 'wind_ods_fact_wtg_1s_raw'

DONE = '.DONE'

HIVE_DB = "bi"


def input_check(day):
    check_cmd = 'hadoop fs -test -e /merged/nrt/{day}/{done}'.format(day=day, done=DONE)
    print check_cmd
    while True:
        a, b = commands.getstatusoutput(check_cmd)
        if a == SUCCESS:
            return True
        else:
            print datetime.datetime.now(), 'no done'
            time.sleep(60 * 10)


def run_stream_task(day):
    rmr_cmd = 'hadoop fs -rmr {output}/{day}'.format(output=HDFS_OUTPUT_PREFIX, day=day)
    print rmr_cmd
    commands.getstatusoutput(rmr_cmd)

    stream_cmd = "hadoop jar {jar} " \
                 "-D mapred.reduce.tasks=0 " \
                 "-D mapreduce.input.fileinputformat.split.maxsize=268435456 " \
                 "-D mapreduce.job.name='standard_mapping_mr_{day}' " \
                 "-files {job_base}/{mapping_file},{job_base}/{standard_file}," \
                 "{job_base}/{wtg_mapping_file},{job_base}/{mapper_file} " \
                 "-input {input}/{day}/* " \
                 "-inputformat org.apache.hadoop.mapred.lib.CombineTextInputFormat " \
                 "-output {output}/{day} " \
                 "-mapper 'python {mapper_file}' ".format(jar=STREAMING_JAR,
                                                          day=day,
                                                          mapping_file=MAPPING_FILE,
                                                          standard_file=STANDARD_FILE,
                                                          wtg_mapping_file=WTG_MAPPING_FILE,
                                                          input=HDFS_INPUT_PREFIX,
                                                          output=HDFS_OUTPUT_PREFIX,
                                                          mapper_file=MAPPER_SCRIPT,
                                                          job_base=JOB_BASE)
    print stream_cmd
    a, b = commands.getstatusoutput(stream_cmd)
    if a != SUCCESS:
        print 'standard_mapping_mr_{day} fail!'.format(day=day)
        print b
        return ERROR_RUN_STREAM_TASK
    else:
        print 'standard_mapping_mr_{day} success!'.format(day=day)
        return SUCCESS


def add_partition(day):
    drop_partition_cmd = 'hive -e "use {db}; alter table {table} drop if exists partition (hp_date=\'{day}\')"'.format(
        db=HIVE_DB, table=TABLE, day=day
    )
    print drop_partition_cmd
    a, b = commands.getstatusoutput(drop_partition_cmd)
    if a != SUCCESS:
        print "drop partition cmd error!"
        print b
        return ERROR_DROP_PARTITION
    else:
        print "drop partition cmd successful!"

    add_partition_cmd = 'hive -e "use {db}; ' \
                        'alter table {table} add partition (hp_date = \'{day}\') ' \
                        'location \'{output}/{day}\'"'.format(
        db=HIVE_DB, table=TABLE, day=day, output=HDFS_OUTPUT_PREFIX
    )
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
    day = (datetime.datetime.fromtimestamp(int(sys.argv[1])) - datetime.timedelta(days=1)).strftime("%Y%m%d")
    if input_check(day):
        res = run_stream_task(day)
        if res != SUCCESS:
            exit(res)
        res = add_partition(day)
        if res == SUCCESS:
            print "task successful!"
        else:
            print "task fail!"
        exit(res)
