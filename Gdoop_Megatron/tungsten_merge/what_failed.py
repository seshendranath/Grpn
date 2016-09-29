#!/usr/bin/env python

"""
Usage:
  main.py  --tungsten_conf=<conf_file> --db_conf=<conf_file> --date_id=<date> [--status=<status>] [--to=<to>]    [-v|-vv] 
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

from shared import os_util
from shared  import hdfs
from shared  import hive_util
from shared  import yaml_util
from shared.sql  import *
from mysqlInfo import MysqlInfo
from merge_tab import *

from config import *
from util import *


def main(args):
    """ create zr yml file for all tables """

    conf = load_conf(args)

    conf = get_conf()

    service_name = get_service_name()
    loc = conf['stage_location']
    work_dir = conf['work_dir']
    date_id = conf['--date_id']
    dsn = conf['dsn']
    status = conf.get('--status', 'FAILED')
    if not status:
        status = 'FAILED'
    from etlstatusdb import EtlStatusDb
    etl_db = EtlStatusDb(conf['etl_status_dsn'])
    result = etl_db.get_all_inst(service_name, status, date_id )

    mail_body = ""
    for row in result:
        (table_name, process_type, run_id) = row
        mail_body +=  "\n--------------------------------------------------------------------------------------"
        mail_body +=  "\n\t{status} service_name={service_name} table_name={table_name} process_type={process_type} run_id={run_id}".format(**locals())
        table_name = table_name.replace(dsn+"_", dsn+".")
        mail_body +=  "\n\t log_name={work_dir}/{run_id}/{dsn}/{table_name}.log".format(**locals())
        result = etl_db.get_failed_parts_for_run_id(service_name, table_name, run_id)
        if result:
            mail_body += "\n\t failed parts %r"%result
    to = conf['--to']
    print mail_body
    if to and mail_body:
        email(to, "Tungsten ETL fail Summary for %s"%service_name, mail_body)

if __name__ == '__main__':
    args = docopt(__doc__)

    level_map = {0: logging.WARNING, 1: logging.INFO, 2: logging.DEBUG}
    format = '%(levelname)s [%(name)s:%(lineno)d] - %(message)s'
    logging.basicConfig(level=level_map[args['-v']], format=format)
    logging.debug("Args %s"% args)
    
    main(args)
    



