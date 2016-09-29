### Ad-Hoc, to parse Hadoop Job Logs and analyze resources that Megatron is eating up

import re
import fnmatch
import os
import datetime

def extract_info(file_name):

  print "Processing File: ", file_name

  job_id_submit_time_pattern = re.compile("^Job JOBID=\"(job_\d+_\d+)\".*SUBMIT_TIME=\"(\d+)\".*$")
  total_maps_reduces_pattern = re.compile("^.*TOTAL_MAPS=\"(\d+)\" TOTAL_REDUCES=\"(\d+)\".*$")
  finish_time_pattern = re.compile("^Job JOBID=\"(job_\d+_\d+)\" FINISH_TIME=\"(\d+)\".* COUNTERS=.*HDFS\: Number of read operations\)\((\d+)\).*HDFS: Number of large read operations\)\((\d+)\).*HDFS: Number of write operations\)\((\d+)\).*CPU time spent \\\\\\\\\(ms\\\\\\\\\)\)\((\d+)\).*Physical memory \\\\\\\\\(bytes\\\\\\\\\) snapshot\)\((\d+)\).*Virtual memory \\\\\\\\\(bytes\\\\\\\\\) snapshot\)\((\d+)\)")

  for line in open(file_name):
    line = line.strip('\n').strip('\r').strip('\t')
    m1 = job_id_submit_time_pattern.match(line)
    m2 = total_maps_reduces_pattern.match(line)
    m3 = finish_time_pattern.match(line)
    if m1:
      job_id = m1.group(1)
      submit_time = m1.group(2)
    elif m2:
      total_maps = m2.group(1)
      total_reduces = m2.group(2)
    elif m3:
      finish_time = m3.group(2)
      hdfs_read_ops = m3.group(3)
      hdfs_large_read_ops = m3.group(4)
      hdfs_write_ops = m3.group(5)
      cpu = m3.group(6)
      pm = m3.group(7)
      vm = m3.group(8)

  st = datetime.datetime.fromtimestamp(int(submit_time[0:10])).strftime('%Y-%m-%d %H:%M:%S')
  et = datetime.datetime.fromtimestamp(int(finish_time[0:10])).strftime('%Y-%m-%d %H:%M:%S')
  time_took_in_sec = int(finish_time[0:10]) - int(submit_time[0:10])
  fo = open("data", "a")
  fo.write("%s\t%s\t%s\t%s\t%s\t%d\t%s\t%s\t%s\t%s\t%s\t%s\n" % (job_id, st, total_maps, total_reduces, et, time_took_in_sec, hdfs_read_ops, hdfs_large_read_ops, hdfs_write_ops,cpu, pm, vm))
  fo.flush()
  fo.close()


matches = []
for root, dirnames, filenames in os.walk('/var/groupon/hadoop/logs/history/done/pit-prod-etljob1.snc1_1457195931544_/2016/06'):
    for filename in fnmatch.filter(filenames, 'job_*'):
        if "megatron" in filename and filename[-4:] != ".xml":
            matches.append(os.path.join(root, filename))

for match in matches:
  extract_info(match)