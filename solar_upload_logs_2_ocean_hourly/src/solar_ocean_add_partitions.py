import commands
import datetime
import sys

SUCCESS = 0
ERROR_GET_HDFS_PATH = 1
ERROR_EXE_ADD_PARTITION = 2


def get_hdfs_path(hdfs_root, day):
    solar_path = "/%s/%s" % (hdfs_root, "bi/logs/solar")
    yesterday = (datetime.datetime.strptime(day, "%Y%m%d") + datetime.timedelta(days=-1)).strftime('%Y%m%d')
    the_day_before_yesterday = (datetime.datetime.strptime(day, "%Y%m%d") + datetime.timedelta(days=-2)).strftime(
        '%Y%m%d')
    grep_cmd = "hadoop fs -ls -R {path} | grep '{day}$\|{yesterday}$\|{the_day_before_yesterday}$'" \
        .format(path=solar_path, day=day, yesterday=yesterday, the_day_before_yesterday=the_day_before_yesterday)
    print grep_cmd
    a, b = commands.getstatusoutput(grep_cmd)
    if a != SUCCESS:
        print "getHDFSPath error"
        exit(ERROR_GET_HDFS_PATH)
    return [i.split(" ")[-1] for i in b.split("\n")]


def generate_hql(paths, hive_db, day, hour):
    with open("{day}_{hour}_add_partitions.hql".format(day=day, hour=hour), "w") as f:
        f.write("use %s;\n" % hive_db)
        for p in paths:
            tmp = p.strip().split("/")
            table, site, day = tmp[-3], tmp[-2], tmp[-1]
            site = site.replace('=', '="') + '"'
            day = day.replace('=', '="') + '"'
            f.write('alter table {table} add if not exists partition({site}, {day}) location \'{loc}\';\n'.format(
                table=table, site=site, day=day, loc=p))


def exe_add_partition(day, hour):
    hive_cmd = "hive -f {day}_{hour}_add_partitions.hql".format(day=day, hour=hour)
    a, b = commands.getstatusoutput(hive_cmd)
    if a != SUCCESS:
        print "exeAddPartition error"
        exit(ERROR_EXE_ADD_PARTITION)
    else:
        rm_cmd = "rm {day}_{hour}_add_partitions.hql".format(day=day, hour=hour)
        commands.getstatusoutput(rm_cmd)
        exit(SUCCESS)


def add_partition(*argv):
    day, hour = (datetime.datetime.fromtimestamp(int(argv[0])) - datetime.timedelta(hours=9)).strftime(
        '%Y%m%d %H').split(" ")
    hive_db = argv[1]
    hdfs_root = argv[2]

    paths = get_hdfs_path(hdfs_root, day)
    generate_hql(paths, hive_db, day, hour)
    exe_add_partition(day, hour)


# cmd example:   solar_add_partitions.py 1452571200 bi_dev dev
if __name__ == "__main__":
    add_partition(*tuple(sys.argv[1:]))
