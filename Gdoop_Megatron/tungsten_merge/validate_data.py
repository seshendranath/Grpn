#!/usr/bin/env python

"""
Usage:
  validate_data.py  --tungsten_conf=<conf_file> --db_conf=<conf_file> [--tab=<table_name>] [--to=<email>]   [-v|-vv] 
  validate_data.py (-h|--help)

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

from shared import os_util
from shared  import hdfs
from shared  import hive_util
from shared  import yaml_util
from shared.sql  import *
from mysqlInfo import MysqlInfo

from config import *
from util import *
from hiveInfo import *
from merge_tab import *


TabInfo = namedtuple('TabInfo', ['schema', 'tab', 'create_time'])


def get_row_count(conn, sql):
     with conn.get_cursor() as cur:
         cur.execute(sql)
         row = cur.fetchone()
     return row[0]

def table_row_count(mysqlInfo, tab):

    conf = get_conf()
    full_table_name = tab.schema+"."+tab.tab
    conf['--tab'] = full_table_name
    hive_schema = conf['hive_schema']
    hiveInfo = HiveInfo(hive_schema)
    td_schema = conf['teradata_final_schema']
    td_name = td_tab_name()
    tab_schema = tab.schema
    tab_name = tab.tab


    conn = SQL(dsn=conf['teradata_dsn'], adapter="Teradata")
    sql = "SELECT count(1) from {td_schema}.{td_name}".format(**locals())

    td_row_count = get_row_count(conn, sql)
        

    conn = SQL(dsn=conf['dsn'], adapter="MySQLDirect")
    sql = "SELECT count(1) from {full_table_name}".format(**locals())
    mysql_row_count = get_row_count(conn, sql)

    #conn = SQL(dsn=conf['dsn'], adapter="MySQLDirect")
    #sql = "SELECT  table_rows FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{tab_schema}' and TABLE_NAME ='{tab_name}'".format(**locals())
    #mysql_row_count_estimate = get_row_count(conn, sql)

    hive_row_count = hiveInfo.get_row_count(final_tab_name())

    if hive_row_count:
        hive_row_count=int(hive_row_count)
    else:
        hive_row_count = 0

    etl_db = EtlStatusDb(conf['etl_status_dsn'])

    status = "SUCCESS"
    merge_last_part = etl_db.get_last_success_part(conf['service_name'], hive_tab(), "merge")
    load_last_part = etl_db.get_last_success_part(conf['service_name'], hive_tab(), "load")
    if merge_last_part == load_last_part:
        if hive_row_count != td_row_count:
            status = "FAILED"
            
        
    diff_pct = ((mysql_row_count - hive_row_count)*100.0)/mysql_row_count
    if diff_pct > 10:
        status = "FAILED"
    str = "\n\nmysql_row_count={mysql_row_count} hive_row_count={hive_row_count} td_row_count={td_row_count}\n\t merge_last_part={merge_last_part} load_last_part={load_last_part} status={status}\n\n".format(**locals())

    return (status, str)

        
def main(args):    
    conf = load_conf(args)
    mysqlInfo = MysqlInfo(conf['dsn'])

    table_name = conf['--tab']

    include_list =  []

    if table_name:
        include_list.append(table_name)

    tab_list = mysqlInfo.get_table_list(conf.get('include_table',include_list), conf.get('exclude_table', []))

    final_status = "SUCCESS"
    mail_body = " "

    for tab in tab_list:
        (status, str ) = table_row_count(mysqlInfo, tab)
        if status == "FAILED":
            final_status = status
        mail_body += str



    print mail_body
    to = conf['--to']
    if final_status == "FAILED" and to:
        email(to, "Data validation FAILED %s"%get_service_name(), mail_body)
        


if __name__ == '__main__':

    args = docopt(__doc__)

    level_map = {0: logging.WARNING, 1: logging.INFO, 2: logging.DEBUG}
    format = '%(levelname)s [%(name)s:%(lineno)d] - %(message)s'
    logging.basicConfig(level=level_map[args['-v']], format=format)
    logging.debug("Args %s"% args)
    
    main(args)
    



