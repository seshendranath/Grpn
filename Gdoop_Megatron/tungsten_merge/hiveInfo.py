#!/usr/bin/env python


from collections import Counter, namedtuple
from docopt import docopt
import logging
import os
import re
import sys
import yaml
import tempfile
import datetime
import subprocess
import MySQLdb
import shlex

from shared import os_util
from shared  import hdfs
from shared  import hive_util
from shared  import hdfs
from shared  import yaml_util
from shared.sql  import *


TabInfo = namedtuple('TabInfo', ['schema', 'tab', 'create_time'])
SchemaInfo = namedtuple('SchemaInfo' , [ 'key_list', 'col_list', 'col_hash' ])
timestamp_cols = ['updated_at','created_at']

def lsrd(path, last_proc_time):
    """
    Executes 'hadoop fs -lsr [path]' and returns a recursive list of
    fully-qualified subpaths.
    """
    from datetime import datetime
    cmd = [hdfs.get_hadoop(), "fs", '-ls', '-R', path]
    (result, stdout, stderr) = os_util.execute_command(cmd)
    if result is not 0:
        logging.info("No files for "+path)
        return []

    # the lsr output looks like:
    # -rw-r--r--   3 chi supergroup        365 2013-01-25 23:30 /user/chi/testout.txt/part-00005
    # we want the fourth position as the size and
    # the seventh position as the filename
    tups = [line.strip().split() for line in stdout.split('\n')
            if line.startswith('d') and line.strip()]

    res=[]
    for tup in tups:
        if datetime.strptime(tup[-3]+' '+tup[-2]+':00', "%Y-%m-%d %H:%M:%S" ) >= last_proc_time:
            res.append(tup[-1])
    return res

def run_hive_sql(hsql):
    logging.info("Running hive ql "+hsql)

    with os_util.TemporaryFile() as hivefile:
        hivefile.write(hsql.strip(";") + ";\n")

        hivefile.flush()
        cmd = [os_util.which("hive"), "-f", hivefile.name]

        result, stdout, stderr = os_util.execute_command(cmd, do_sideput=True)

        if stderr:
            logging.info("stderr="+stderr)
        # check results
        if result is not 0:
            logging.error("query failed "+hsql)
            raise Exception("query failed "+hsql)
    return stdout


class HiveInfo:
    def __init__(self, schema):
        self.schema = schema

    def get_tab_schema_detail(self, table_name):
        """ executes hive describe formatted <table_name>'. Returns map of results
            Also and date formats: Wed Mar 11 20:07:23 UTC 2015
        """
        kvre = re.compile(r'(\w+): (.*)$')
        timere = re.compile(r'\w+ (\w+) (\d+) (\d+):(\d+):(\d+) \w+ (\d+)')
        
        cmd = [hive_util.get_hive(), "-e",  '"use %s;describe formatted %s"'%(self.schema, table_name)]
        logging.info("Running "+" ".join(cmd))
        schema_hash = {}
        schema_hash['cols'] = {}
        schema_hash['part_cols'] = {}
        schema_hash['details'] = {}
        schema_hash['storage'] = {}
        (result, stdout, stderr) = os_util.execute_command(cmd, do_sideput=True)
        if result != 0:
            return schema_hash
        cols = []
        part_info = "# Partition Information"
        deatiled_info = "# Detailed Table Information"
        storage_info = "# Storage Information"
        curr_key = 'cols'
        for line in stdout.split('\n'):
            line = line.strip()
            if not line:
                continue
            if line[0] == "#":
                if part_info in line:
                    curr_key = 'part_cols'
                if deatiled_info in line:
                    curr_key = 'details'
                if storage_info in line:
                    curr_key = 'storage'
                continue
            kv = kvre.match(line)
            if kv:
                parm = kv.group(1).lower().strip()
                val = kv.group(2).strip()
                tval_s = timere.match(val)
                if tval_s:
                    m = tval_s.group(1)
                    d = tval_s.group(2)
                    Y = tval_s.group(6)
                    H = tval_s.group(3)
                    M = tval_s.group(4)
                    S = tval_s.group(5)
                    datestr = "{m} {d} {Y} {H}:{M}:{S}".format(**locals())
                    val = datetime.datetime.strptime(datestr, "%b %d %Y %H:%M:%S")
                
                schema_hash[curr_key][parm] = val

        logging.info("table {table_name} desc: {stdout}".format(**locals()) )
        return schema_hash

    def get_table_schema(self, table_name):
        """ executes hive describe <table_name>'. Returns results"""
        cols = []
        cmd = [hive_util.get_hive(), "-e",  "use %s;describe %s"% (self.schema, table_name)]
        logging.info("Running hive cmd %s"%" ".join(cmd))
        (result, stdout, stderr) = os_util.execute_command(cmd, do_sideput=True)
        if result != 0:
            logging.info("failed run  stdout="+stdout+" stderr="+stderr)
            return []
        if "Table not found" in stdout:
            logging.info("table does not exists  stdout="+stdout+" stderr="+stderr)
            return []
        for line in stdout.split('\n'):
            print >>sys.stderr, line
            if "WARN" not in line:
              name, type = shlex.split(line)
              cols.append((name, type))
        return cols

    def get_row_count(self, table_name):
        """ get_row_count """
        cmd = [hive_util.get_hive(), "-e",  "use %s;select count(1) from %s"% (self.schema, table_name)]
        (result, stdout, stderr) = os_util.execute_command(cmd, do_sideput=True)
        if result != 0:
            logging.info("failed run  cmd="+`cmd`+" stdout="+stdout+" stderr="+stderr)
            return ""
            
        return stdout.strip()

    def get_aggregate(self, hdfs_temp_dir, aggr_name, asString=True, getMax=True):
        aggrResult = None
        valTypeArg = "" if asString else "-n"
        orderArg = "" if getMax else "-r"
        cmd = [hdfs.get_hadoop(), "fs", "-cat", "%s/%s_* | sort  %s %s | tail -1" % (hdfs_temp_dir, aggr_name, valTypeArg, orderArg)]
        logging.info("Running hadoop cmd %s"%" ".join(cmd))
        (result, stdout, stderr) = os_util.execute_command(cmd, do_sideput=True, shell=True)
        
        if result is not None and stdout.strip() != 'NULL':
            if result != 0:
                logging.info("failed run  cmd="+`cmd`+" stdout="+stdout+" stderr="+stderr)
                aggrResult = ""
            else:
                aggrResult = stdout.strip()
        
        return aggrResult

    def get_counter_value(self, hdfs_temp_dir, counter_name):
        total = 0
        cmd = [hdfs.get_hadoop(), "fs", "-cat", "%s/%s_* | awk '{ sum += $1 } END { print sum }' " % (hdfs_temp_dir, counter_name)]
        logging.info("Running hadoop cmd %s"%" ".join(cmd))
        (result, stdout, stderr) = os_util.execute_command(cmd, do_sideput=True, shell=True)
        
        if result is not None and stdout.strip() != 'NULL':
            if result != 0:
                logging.info("failed run  cmd="+`cmd`+" stdout="+stdout+" stderr="+stderr)
            else:
                total = int(stdout.strip())

        return total
        
    def get_latest_update_time(self, history_table, pkid, hdfs_temp_dir = None):
        """ get_row_count """
        """ hdfs_temp_dir is specified for merge and load operations. None is used for sqoop"""
        
        maxUpdateAt = None
        maxPKID = 0
        minPKID = 0
        result = None 
        if hdfs_temp_dir: 
            maxUpdateAt = self.get_aggregate(hdfs_temp_dir, "max_date")
            maxPKID = self.get_aggregate(hdfs_temp_dir, "max_pkid", asString = False)
            minPKID = self.get_aggregate(hdfs_temp_dir, "min_pkid", asString = False, getMax = False)
            logging.info("get_aggregate: (%s, %s, %s)" % (maxUpdateAt, minPKID, maxPKID))
        else:
            cols = self.get_table_schema(history_table)
            col_names = [nm for (nm, ty) in cols]
            max_id_func = "max(cast(%s as int))" % pkid
            max_time_col = "'0000-00-00 00:00:00'"
            for col in timestamp_cols:
                if col in zip(*cols)[0]:
                    max_time_col = "max(%s)" % col
                    break

            cmd = [hive_util.get_hive(), "-e",  "use %s; select CONCAT(%s, ', ', %s ) from %s limit 1" % (self.schema, max_id_func, max_time_col, history_table)]
            logging.info("Running hive cmd %s"%" ".join(cmd))
            (result, stdout, stderr) = os_util.execute_command(cmd, do_sideput=True)
            
            if result is not None and stdout.strip() != 'NULL':
                if result != 0:
                    logging.info("failed run  cmd="+`cmd`+" stdout="+stdout+" stderr="+stderr)
                    maxUpdateAt = ""
                else:
                    for line in stdout.split('\n'):
                        if "WARN" not in line:
                            maxList = line.strip().split(",")
                    if not maxList:
                        maxList=['0', '0000-00-00 00:00:00']
                    maxPKID = maxList[0]
                    maxUpdateAt = maxList[1]
            logging.info("get_aggregate. ELSE: (%s, %s, %s)" % (maxUpdateAt, minPKID, maxPKID))
        return (maxUpdateAt, int(minPKID), int(maxPKID))

    def table_partitions(self, table_name):
        """ executes hive describe <table_name>'. Returns results"""
        cmd = [hive_util.get_hive(), "-e",  "use %s;show partitions %s"% (self.schema, table_name)]
        (result, stdout, stderr) = os_util.execute_command(cmd, do_sideput=False)
        if result != 0:
            logging.info("failed run  stdout="+stdout+" stderr="+stderr)
            return []
        return [ line.strip() for line in stdout.split('\n') ]


            




