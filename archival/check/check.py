#!/usr/bin/env python

"""
Usage:
  check.py --source=<schema>.<table> --target=<schema>.<table> [--num_checks=<num_random_checks>]
            [--limit=<limit>] [--cmp_row_row] [--debug] [-v|-vv]
  check.py (-h | --help)

Options:
  --source=<schema>.<table>                         Specify Source(Staging) Table
  --target=<schema>.<table>                         Specify Target(Final) Table
  --num_checks=<num_random_checks>                  Specify num of random checks to perform [default: 12]
  --limit=<limit>                                   Specify limit of rows to check [default: 100]
  --cmp_row_row                                     Compare row to row
  -v|-vv                                            Verbose Level
"""


from docopt import docopt
from config import *
from email.mime.text import MIMEText
import shlex
import smtplib
import re
import random

import sys
sys.path.append('../')
from os_util import *


def exec_hive_query(query):
    cmd = [get_hive(), "-e",  '"%s"' % query]
    iterations = 1

    for i in range(0, iterations):
        logging.info("Running "+" ".join(cmd))
        (result, stdout, stderr) = execute_command(cmd)
        if result != 0:
            raise Exception("Process ended abnormally: [%s]" % stderr)
        else:
            return result, stdout, stderr

    # logging.info("...Sending email unable to execute HIVE")
    # send_email('HIVE ERROR', 'unable to execute HQL repeatedly %s \n STDOUT: %s \n STDERR: %s '
    #                   % (query, stdout, stderr))


def exec_hive_file(file):
    cmd = [get_hive(), "-f",  '"%s"' % file]
    logging.info("Running "+" ".join(cmd))
    (result, stdout, stderr) = execute_command(cmd)
    if result != 0:
        raise Exception("Process ended abnormally: [%s]" % stderr)
    else:
        num_rows = 0
        output = stdout + "\n" + stderr
        for line in output.splitlines():
            m = re.search('numRows=(\d+),', line)
            if m:
                num_rows += int(m.group(1))
        logging.info("Hive File executed Successfully")
        logging.info("NUM_ROWS:%s" % str(num_rows))
        set('target_count', num_rows)
        return result


def send_email(subject, message):
    msg = MIMEText(message)  # Create a text/plain message

    sender = 'edw-infra@groupon.com'
    recipients = ['aguyyala@groupon.com']
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = ", ".join(recipients)

    s = smtplib.SMTP('localhost')
    s.sendmail(sender, recipients, msg.as_string())
    s.quit()


def table_schema(db, table):
    cmd = [get_hive(), "-S", "-e",  '"use %s;describe formatted %s"' % (db, table)]
    logging.info("Running "+" ".join(cmd))
    schema_hash = dict()
    schema_hash['cols'] = {}
    schema_hash['part_cols'] = {}
    schema_hash['details'] = {}
    schema_hash['storage'] = {}
    schema_hash['cols_order'] = []
    schema_hash['part_cols_order'] = []
    (result, stdout, stderr) = execute_command(cmd)
    if result != 0:
        return schema_hash
    part_info = "# Partition Information"
    detailed_info = "# Detailed Table Information"
    storage_info = "# Storage Information"
    curr_key = 'cols'
    start = True
    for line in stdout.split('\n'):
        line = line.strip()
        if not line or (line[0] != "#" and start):
            continue
        start = False
        if line[0] == "#":
            if part_info in line:
                curr_key = 'part_cols'
            if detailed_info in line:
                curr_key = 'details'
            if storage_info in line:
                curr_key = 'storage'
            continue
        arr = shlex.split(line)
        if len(arr) >= 2:
            if curr_key in ('cols', 'part_cols'):
                schema_hash[curr_key+"_order"].append(arr[0])
            schema_hash[curr_key][arr[0]] = arr[1]
    return schema_hash


def set_schema():
    schema = table_schema(get('target_db'), get('target_table'))
    if schema.get('cols_order'):
        set('tgt_cols', ",".join(schema['cols_order']))
        set('tgt_part_cols',  ",".join(schema['part_cols_order']))
        set('tgt_cols_schema', ",".join([str(i)+" "+str(schema['cols'][i]) for i in schema['cols_order']]))
        set('tgt_part_cols_schema',",".join([str(i)+" "+str(schema['part_cols'][i]) for i in schema['part_cols_order']]))
        set('src_count_col', ",".join(["count(distinct " + i + ")" for i in schema['cols_order']]))
        set('tgt_hdfs_location', schema.get('details').get('Location:'))
    else:
        raise Exception("Target Table doesn't exist")

    if "OrcInputFormat" not in schema.get('storage').get('InputFormat:'):
        raise Exception("Target Table NOT in ORC: %s" % schema.get('storage').get('InputFormat:'))

    schema = table_schema(get('source_db'), get('source_table'))
    if schema.get('cols_order'):
        set('src_cols', ",".join(schema['cols_order']))
        set('src_part_cols',  ",".join(schema['part_cols_order']))
        set('src_cols_schema', ",".join([str(i)+" "+str(schema['cols'][i]) for i in schema['cols_order']]))
        set('src_part_cols_schema',",".join([str(i)+" "+str(schema['part_cols'][i]) for i in schema['part_cols_order']]))
        set('tgt_count_col', ",".join(["count(distinct " + i + ")" for i in schema['cols_order']]))
        set('src_hdfs_location', schema.get('details').get('Location:'))
    else:
        raise Exception("Source Table doesn't exist")

    src_schema = get('src_cols_schema')+","+get('src_part_cols_schema')
    tgt_schema = get('tgt_cols_schema')+","+get('tgt_part_cols_schema')
    if src_schema != tgt_schema:
        raise Exception("Source and Target Table schema MISMATCH: %s %s" % (src_schema, tgt_schema))


def data_quality(part):
    for i in range(0, int(get('--num_checks'))):
        cur_part = random.choice(part)
        if get('--cmp_row_row'):
            src_query = "SELECT * FROM %s WHERE %s LIMIT %s" % (get('--source'), cur_part, get('--limit'))
            tgt_query = "SELECT * FROM %s WHERE %s LIMIT %s" % (get('--target'), cur_part, get('--limit'))
        else:
            src_query = "SELECT %s FROM %s WHERE %s" % (get('src_count_col'), get('--source'), cur_part)
            tgt_query = "SELECT %s FROM %s WHERE %s" % (get('tgt_count_col'), get('--target'), cur_part)

        (src_result, src_stdout, src_stderr) = exec_hive_query(src_query)
        (tgt_result, tgt_stdout, tgt_stderr) = exec_hive_query(tgt_query)
        src_output = tgt_output = ""

        for line in src_stdout.splitlines():
            if "WARN" in line:
                continue
            src_output += line

        for line in tgt_stdout.splitlines():
            if "WARN" in line:
                continue
            tgt_output += line

        if src_output == tgt_output:
            logging.info("CORRECT")
            status = "CORRECT"
        else:
            logging.info("MISMATCH")
            status = "MISMATCH"
            # raise Exception("Source and Target Data MISMATCH")

        if get('--cmp_row_row'):
            logging.info("Src Query: %s | Tgt Query: %s" % (src_query, tgt_query))
            logging.info("Src: %s | Tgt: %s | Partition: %s | Status: %s "
                         % (get('--source'), get('--target'), cur_part, status))
        else:
            logging.info("Src Query: %s | Tgt Query: %s" % (src_query, tgt_query))
            logging.info("Src: %s | Tgt: %s | Partition: %s | Status: %s | Source(%s) = Target(%s)"
                         % (get('--source'), get('--target'), cur_part, status, src_output, tgt_output))


def get_partitions(table_name):
    (result, stdout, stderr) = exec_hive_query("SHOW PARTITIONS %s" % table_name)
    part = []
    for line in stdout.replace("/", "' and ").replace("=", "='").split("\n"):
        if "WARN" in line:
            continue
        part.append(line + "'")
    return part


def start():
    # set_schema()
    part = get_partitions(get('--target'))
    data_quality(part)


if __name__ == '__main__':
    arg = docopt(__doc__)
    conf = load_conf(arg)
    level_map = {0: logging.WARNING, 1: logging.INFO, 2: logging.DEBUG}
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(level_map[conf['-v']])
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    root.addHandler(ch)
    logging.debug('Args [%s]' % conf)

    try:
        start()
    except Exception as e:
        logging.error('ERROR OCCURRED: [%s]' % str(e))
        sys.exit(1)