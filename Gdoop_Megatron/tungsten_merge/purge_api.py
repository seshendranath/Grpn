#!/usr/bin/env python

"""
Usage:
  a.py --run_mode=<mode> --tungsten_conf=<conf_file> --db_conf=<conf_file> --time=<time>  [--service_name=<service_name>]  [--dryrun] [-v|-vv]
  a.py (-h|--help)

Options:
    dryrun enable dry run
    force is used with sqoop to force sqoop
          or with full load to force a full load
"""

# --run_mode=merge
# --tab=groupon_production.orders --tungsten_conf=/home/megatron/tu/tungsten_merge/snc1.orders/tung.yml
# --db_conf=/home/megatron/tu/tungsten_merge/snc1.orders/mysql_dev.yml
# a.py  --tab=groupon_production.orders --tungsten_conf=/home/megatron/tu/tungsten_merge/snc1.orders/tung.yml --db_conf=/home/megatron/tu/tungsten_merge/snc1.orders/mysql_dev.yml
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
import commands
from shared import os_util
from shared  import hdfs
from shared  import hive_util
from shared  import yaml_util
from mysqlInfo import MysqlInfo
from config import *
from util import run_zr, make_dir
import datetime
from time import gmtime, strftime
from mako.template import exceptions,Template
from etlstatusdb import *
from td_util import *
from sql_adaptor import *

from hiveInfo import HiveInfo, lsrd, run_hive_sql


g_script_dir = os.path.dirname(os.path.realpath(__file__))

import MySQLdb
import sys
from collections import namedtuple

g_script_dir = os.path.dirname(os.path.realpath(__file__))
g_conf = None

def get_time():
    return g_conf['--time']

def get_service_name():
    return g_conf['--service_name']

def __get_hostname():
        cmd = ["hostname", "-f"]
        (result, stdout, stderr) = os_util.execute_command(cmd)
        return stdout.strip()
def get_dryrun():
    return g_conf['--dryrun']


def merge_purge_main():

    service_name = get_service_name()
    if not service_name:
        raise Exception( "in merge_purge you need service name")
    etl_db = EtlStatusDb(conf['etl_status_dsn'])
    purge_list=etl_db.get_purge_list(get_service_name(), get_time())

    if not service_name:
        raise Exception( "in merge_purge you need service name")
    if(len(purge_list)>0):
        for hdfs_loc in purge_list:
            cmd = [hdfs.get_hadoop(),'fs', '-rm', '-R',  hdfs_loc]
            print cmd
            if not (get_dryrun()):
                (result, stdout, stderr) = os_util.execute_command(cmd)
                print stderr,stdout
                print result
                if result==0:
                    print "deleting from db"
                    conn = SQL(dsn=conf['etl_status_dsn'], adapter="MySQLDirect")
                    host_name = __get_hostname()
                    sql = "DELETE FROM hdfs_trash WHERE host_name = '{host_name}' and service_name = '${service_name}' AND hdfs_path IN ('${hdfs_loc}') ;".format(**locals())
                    with conn.get_cursor() as cur:
                        cur.execute(sql)


def stg_purge_main():
#./older_than_hadoop.sh 30 svadlamudi /user/svadlamudi
    dryrun_str=None
    if(get_dryrun()):
        dryrun_str="dryrun"

    cmd= [g_script_dir+"/purge_older_file.sh", get_time(), "svc_meg_prod","/user/grp_gdoop_edw_meg_prod/staging/",dryrun_str]
    print cmd
    (result, stdout, stderr) = os_util.execute_command(cmd,do_sideput=True)
    print stdout,stderr
    if result!=0:
        raise
    print stdout,stderr


def main():
    try:
       if get_run_mode() == "stg_purge":
           stg_purge_main()
       elif get_run_mode() == "merge_purge":
            merge_purge_main()
    except Exception as e:
        logging.exception("HARD FAILURE::main() exception trapped: %s" % str(e))


if __name__ == '__main__':
    args = docopt(__doc__)
    level_map = {0: logging.WARNING, 1: logging.INFO, 2: logging.DEBUG}
    format = '%(asctime)s %(levelname)s [%(name)s:%(lineno)d] - %(message)s'
    logging.basicConfig(level=level_map[args['-v']], format=format)

    conf = load_conf(args)
    g_conf=conf

    if get_run_mode() not in ("stg_purge", "merge_purge"):
        raise Exception("Unnkown run_mode %s"%conf["--run_mode"])
    print get_dryrun()
    main()




