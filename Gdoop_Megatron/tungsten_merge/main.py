#!/usr/bin/env python

"""
Usage:
  main.py  --tungsten_conf=<conf_file> --db_conf=<conf_file> --workflow_id=<run_id>  [--run_mode=<mode>]  [--test_mode]  [--dryrun]  [--force] [--create_schema]  [-v|-vv]
  main.py (-h|--help)

Options:
    dryrun enable dry run
    force is used with sqoop to force sqoop
"""

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

from shared import os_util
from shared  import hdfs
from shared  import hive_util
from shared  import yaml_util
from shared.sql  import *
from mysqlInfo import MysqlInfo
from etlstatusdb import EtlStatusDb

from config import get_conf, load_conf, get_run_dir
from util import run_zr, make_dir

g_script_dir = os.path.dirname(os.path.realpath(__file__))

def make_run_dir():
    """ make working dir """

    run_dir = get_run_dir()
    make_dir(run_dir)

def get_yml_file():
    """ get yml file """

    conf = get_conf()
    run_dir = get_run_dir()
    dsn = conf['dsn']
    yml_file = os.path.join(run_dir, dsn+".yml")
    return yml_file

def make_table_run_dir(tab):
    full_tab_name = tab.schema + "."+ tab.tab
    tab_dir = os.path.join(get_run_dir(), full_tab_name)
    make_dir(tab_dir)
    return tab_dir
    
def create_yml(tab_list):
    """ create zr yml file for all tables """

    make_run_dir()

    yml_file = get_yml_file()

    task_hash = {
        "start_task" : { "class" : "NopTask" }
        }

    conf = get_conf()
    run_dir = get_run_dir()
    tab_list = sorted(tab_list)

    with open(yml_file, "w") as fp:
        task_name_list = []
        for tab in tab_list:

            full_tab_name = tab.schema + "."+ tab.tab
            tab_run_dir = make_table_run_dir(tab)
            logging.info("Adding table: %s", full_tab_name)
            task_name = "merge_%s"%full_tab_name

            dryrun=""
            if conf['--dryrun']:
                dryrun = "--dryrun"

            force_mode = ""
            if conf['--force']:
                force_mode = "--force"

            test_mode = ""
            if conf['--test_mode']:
                test_mode = "--test_mode"

            create_schema=""
            if conf['--create_schema']:
                create_schema = "--create_schema"
            
            base_dir = g_script_dir

            tung_conf = conf['--tungsten_conf']

            db_conf=conf['--db_conf']
            id=conf['--workflow_id']
            run_mode=conf["--run_mode"]
            dop = conf['dop']
            if run_mode == "sqoop":
                dop = conf['sqoop_dop']

            if run_mode in ['full_load', 'load']:
                dop = conf.get('load_dop', 4)

            script_cmd = "python {base_dir}/merge_tab.py --run_mode={run_mode} --tab={full_tab_name} --tungsten_conf={tung_conf} --db_conf={db_conf} --workflow_id={id} {dryrun} {test_mode} {force_mode} {create_schema} -vv >> {tab_run_dir}/{full_tab_name}.log 2>&1 ".format(**locals())

            merge_task = {
                    "class" : "ScriptTask" ,
                     "dependencies" : [ "start_task" ],
                     "configuration" :
                        { "script" : script_cmd }
                }

            task_hash[task_name] = merge_task
            task_name_list.append(task_name)
        task_hash['end_task'] = { "class" : "NopTask" , "dependencies" : task_name_list}
        task_hash['settings'] = { "parallelism" : dop}
        yml = yaml_util.dump(task_hash)
        print >>fp, yml

def main(args):    
    conf = load_conf(args)
    mysqlInfo = MysqlInfo(conf['dsn'])
    
    tab_list = mysqlInfo.get_table_list(conf.get('include_table',[]), conf.get('exclude_table', []))

    create_yml(tab_list)

    yml_file = get_yml_file()

    run_zr(yml_file, get_conf())


if __name__ == '__main__':
    args = docopt(__doc__)

    level_map = {0: logging.WARNING, 1: logging.INFO, 2: logging.DEBUG}
    format = '%(levelname)s [%(name)s:%(lineno)d] - %(message)s'
    logging.basicConfig(level=level_map[args['-v']], format=format)
    logging.debug("Args %s"% args)
    
    main(args)
