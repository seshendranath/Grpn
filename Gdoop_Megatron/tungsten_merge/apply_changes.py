#!/usr/bin/env python
"""A more advanced Reducer, using Python iterators and generators."""

from itertools import groupby
from operator import itemgetter
import sys
import subprocess
import datetime
import tempfile
import re

class CountTask(object):
    # test_name:5,15
    def __init__(self, task_desc = None):
        self.name = None
        if task_desc:
            pat = re.compile("(.*):(\d*),(\d*)$")
            m = pat.match(task_desc)
            if m:
                self.name = m.group(1)
                self.low = int(m.group(2))
                self.high = int(m.group(3))
                self.rows = 0
                print >>sys.stderr, "CountTask:: name: {self.name} low: {self.low}  high: {self.high}".format(**locals())
            else:
                raise Exception("Invalid CountTask descriptor %s"%task_desc)

    def conditional_increment(self, pkid):
        if pkid >= self.low and pkid <= self.high:
            self.rows += 1

class Counters(object): 
    # cnt1:3,55/cnt2:45,100
    def __init__(self, count_task_defs):
        self.counter_tasks = []
        for task_desc in count_task_defs.split("/"):
            ctask = CountTask(task_desc)
            if ctask.name:
                self.counter_tasks.append(ctask)
    def inc_counters(self, row):
        pkid = get_pkid(row)
        for task in self.counter_tasks:
            task.conditional_increment(pkid)
    def output_counters(self, suffix):
        for task in self.counter_tasks:
            output_aggregate(task.name, task.rows, suffix)

def format_uuid(val):
    if len(val) < 32:
        val = val.ljust(32, '0')
    uuid = val[:8] + '-' + val[8:12] + '-' + val[12:16] + '-' + val[16:20] + '-' + val[20:32] if len(val) == 32 else val
    return uuid.lower()

def format_date(val):
    return '' if val == "0000-00-00" or len(val) < 8 else val

def format_datetime(val):
    return '' if val == "0000-00-00 00:00:00" or len(val) < 14 else val

DATA_FILTERS = {
    "none" : lambda val: val,
    "uuid" : format_uuid,
    "date" : format_date,
    "datetime" : format_datetime
}

separator = chr(int(sys.argv[1])) 
mode = sys.argv[2]
cols = sys.argv[3].split(",")
keys = sys.argv[4].split(",")
filter_names = sys.argv[5].split(",")
hdfs_temp_dir = sys.argv[6]

count_task_defs = ""
if len(sys.argv) > 7:
    count_task_defs = sys.argv[7]

key_idx_list = [ cols.index(key) for key in keys ]
data_filters = [DATA_FILTERS[name] for name in filter_names]

timestamp_cols = ['updated_at','created_at'] 
timestamp_col_index = []
primarykey_id_index = -1

max_date = ''
min_pkid = 9999999999999999
max_pkid = 0
counters = None

for key,i in enumerate(key_idx_list):
    if key  == -1:
        print >>sys.stderr, "Invalid key" , keys[i]
        sys.exit(1)

print >>sys.stderr, "key index: " , `key_idx_list`

max_key_idx = max(key_idx_list)

def get_keys(data):
    keys = [ data[i+4] for i in key_idx_list ]
    return separator.join(keys)

def get_pkid(data):
    global primarykey_id_index
    if primarykey_id_index >= 0:
        pkid = int(data[primarykey_id_index+4])
    else:
        pkid = -1
    return pkid

def find_timestamp_col_index():
    global timestamp_col_index
    for column in timestamp_cols:
        if column in cols:
            timestamp_col_index.append(cols.index(column)+4)
    timestamp_col_index.append(3)

def find_primarykey_id_index():
    global primarykey_id_index
    pkey_name = keys[0]
    if pkey_name in keys:
        primarykey_id_index = cols.index(pkey_name)
    print >>sys.stderr, "primarykey_id_index:" , primarykey_id_index

def process_row_merge(last_row):
    global counters
    row_op = last_row[0]
    if row_op == "D":
        return
    else:
        new_row = map(lambda x, f: '' if x=='\N' else f(x), last_row[4:], data_filters)
        l = separator.join(new_row)
        if row_op != "T":
            # op of NOT T is from a tungsten increment (D, U, M)
            update_aggregates(last_row)
        print l
        counters.inc_counters(last_row)

def process_row_diff(last_row):
    row_op = last_row[0]
    row_op = row_op if row_op == "D" else "M"
    new_row = map(lambda x, f: '' if x=='\N' else f(x), last_row[4:], data_filters)
    new_row.append(row_op)
    new_row.append(last_row[3])
    l = separator.join(new_row)
    print l
    update_aggregates(last_row)

def output_aggregates():
    global counters
    suffix ='_'+(datetime.datetime.now().strftime("%y%m%d_%H%M%S"))
    output_aggregate('max_date', max_date, suffix)
    output_aggregate('min_pkid', min_pkid, suffix)
    output_aggregate('max_pkid', max_pkid, suffix)
    if counters:
        counters.output_counters(suffix)

def output_aggregate(aggr_name, aggr_value, suffix):    
    f = tempfile.NamedTemporaryFile(prefix= '%s_'%aggr_name, suffix=suffix, dir='/tmp')
    f.write("%s\n" % aggr_value)
    f.seek(0)
    command = "hadoop fs -put %s %s" % (f.name, hdfs_temp_dir)
    p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    p.wait()
    f.close()
    (stdout, stderr) = p.communicate()
    print >>sys.stderr, stdout, stderr
    print >>sys.stderr, "aggr_value: {aggr_value}".format(**locals())

def update_aggregates(data):
    global max_date
    global min_pkid
    global max_pkid
    
    for idx in timestamp_col_index:
        if format_datetime(data[idx]) != '':
            cur_date = format_datetime(data[idx])
            break
    if cur_date > max_date:
        max_date = cur_date

    cur_pkid = get_pkid(data)
    if cur_pkid >= 0:
        if cur_pkid < min_pkid:
            min_pkid = cur_pkid
        if cur_pkid > max_pkid:
            max_pkid = cur_pkid

def main():
    global counters
    # input comes from STDIN (standard input)
    last_key = None
    last_row = []
    find_timestamp_col_index()
    find_primarykey_id_index()

    process_row = process_row_diff if mode == "diff" else process_row_merge
    
    if mode == "merge":
        counters = Counters(count_task_defs)
        
    for line in sys.stdin:
        data = line.rstrip("\n").split(separator)
        if len(data) <= max_key_idx+4:
            print >>sys.stderr, "invalid row=", line.rstrip("\n") , "num columns=", len(data), "max key=", max_key_idx+4
            continue

        cur_key = get_keys(data)
        if last_key is not None and last_key !=  cur_key:
            process_row(last_row)

        last_key = cur_key
        last_row = data

    if last_key is not None:
        process_row(last_row)

    output_aggregates()

if __name__ == "__main__":
    main()

