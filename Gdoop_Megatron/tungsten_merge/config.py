#!/usr/bin/env python

"""
Usage:
  main.py  --tungsten_conf=<conf_file> --db_conf=<conf_file> --workflow_id=<run id>   [--dryrun]   [-v|-vv] 
  main.py (-h|--help)

Options:
    dryrun enable dry run
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

from shared  import yaml_util

g_conf = None

def get_run_dir():
    conf = get_conf()
    work_dir = conf['work_dir']
    dsn = conf['dsn']
    workflow_id = conf['--workflow_id']
    run_dir = os.path.join(work_dir, workflow_id, dsn)
    return run_dir

def read_table_config(db_conf, tab):
    tabOnly = tab.split(".")[1]
    tconfig = {"table_name" : tabOnly}
    include_tab_list = db_conf.get("include_table" , [])
    for elem in include_tab_list:
        if type(elem) is dict:
            key = elem.keys()[0]
            if key == tab:
                tconfig.update(elem.values()[0])
                break
    return tconfig
     
def load_conf(args):
    global g_conf
    conf_file = args["--tungsten_conf"]
    with open(conf_file, "r") as fp:
        g_conf = yaml_util.load(fp)
    conf_file = args["--db_conf"]
    with open(conf_file, "r") as fp:
        db_conf = yaml_util.load(fp)
        g_conf.update(db_conf)
        if "--tab" in args:
            tab = args["--tab"]
            table_config = read_table_config(db_conf, tab)
            g_conf.update(table_config)
    
    g_conf.update(args)
    return g_conf

def get_conf():
    return g_conf

def get_column_override(colname):
    overrides = g_conf.get('column_overrides', {})
    return overrides.get(colname, None)        
    
def get_run_mode():
    return g_conf['--run_mode']

def get_repair():
    return g_conf.get('--repair', None)

def get_run_id():
    return g_conf['--workflow_id']

def get_service_name():
    return g_conf['service_name']

def get_cluster_name():
    return g_conf.get('cluster_name', '')

def get_megatron_runtime_loc():
    return g_conf.get('megatron_runtime_loc','/tmp')

def get_table_limits_dsn():
    return g_conf.get('table_limits_dsn', None)
