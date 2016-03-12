import Queue
import commands
import datetime
import os
import sys
import threading

SUCCESS = 0


def ip_execute(base_dir, ip, timestamp, normal=0):
    path = base_dir + os.sep + ip
    if normal == 0:
        path += os.sep + 'normal'
    else:
        path += os.sep + 'history'
    exe_cmd = 'cd {dir} && python upload_logs_2_s3_hourly.py {ts}'.format(dir=path, ts=timestamp)
    print exe_cmd
    res, msg = commands.getstatusoutput(exe_cmd)
    q.put((dir, res, msg))


q = Queue.Queue()


def batch_process(threads):
    for t in threads:
        t.start()
    for t in threads:
        t.join()


def execute(*argv):
    timestamp = argv[0]
    normal = int(argv[1])
    day, hour = (datetime.datetime.fromtimestamp(int(timestamp)) - datetime.timedelta(hours=9)).strftime(
        '%Y%m%d %H').split(' ')
    base_dir = commands.getoutput('pwd')
    ls_ips_cmd = "ls | grep ip".format(base=base_dir)
    ips = commands.getoutput(ls_ips_cmd)

    threads = []
    cnt = 0
    for ip in ips.split('\n'):
        t = threading.Thread(target=ip_execute, name='thread-' + ip, args=(base_dir, ip, timestamp, normal))
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
            print("{ip} occurs {error} error".format(ip=item[0], error=item[2]))
    if return_code == 0:
        print("{day}.{hour} task success!".format(day=day, hour=hour))
    exit(return_code)


# cmd example:   python upload_logs_2_s3_hourly_total.py `date +%s` 0
if __name__ == "__main__":
    execute(*tuple(sys.argv[1:]))
