#!/usr/bin/env python

"""
Usage:
  RunReplicator.py --source=<schema>.<table> --target=<schema>.<table>  [-v|-vv]
  RunReplicator.py (-h | --help)

Options:
  --source=<schema>.<table>                         Specify Source(PETL) Table
  --target=<schema>.<table>                         Specify Target(Cerebro V2) Table
  -v|-vv
"""

from docopt import docopt
from config import *
from os_util import *
from email.mime.text import MIMEText

import re
import time
import shlex
import smtplib
import sys


def create_manifest():
    fo = open(get('script_file_name'), "wb")
    fo.write("bteq <<EOT\n")
    fo.write(".SET RECORDMODE OFF\n.SET WIDTH 60000\n.set separator '|'\n.set TITLEDASHES OFF\n.set ECHOREQ ON\n")
    fo.write(".logon %s\n" % get('source'))
    fo.write(".os rm %s\n" % get('local_manifest'))
    fo.write(".export file='%s'\n" % get('local_manifest'))
    fo.write("select current_timestamp,CAST(count(*) AS BIGINT) FROM %s %s;\n" % (get_schema_table(), get('where')))
    if args['--view']:
    	fo.write("CREATE VOLATILE TABLE %s AS (SELECT * FROM %s) WITH NO DATA NO PRIMARY INDEX;\n" % (get_table(), get_schema_table()))
        fo.write("SHOW TABLE %s;\n" % (get_table()))
    else:
    	fo.write("SHOW TABLE %s;\n" % (get_schema_table()))
    fo.write(".EXPORT RESET;\n.QUIT;\n")
    fo.write("EOT\n")
    fo.flush()
    fo.close()
    logging.info("Creating Manifest file %s" % get('local_manifest'))
    cmd = [which('sh'), get('script_file_name')]
    (result, stdout, stderr) = execute_command(cmd)
    if result is not 0:
        raise Exception("Process ended abnormally: [%s]" % stderr)
    return result, stdout, stderr


def table_schema():
    cmd = [get_hive(), "-S", "-e",  '"use %s;describe formatted %s"'%(get_hive_schema(), get_hive_table())]
    logging.info("Running "+" ".join(cmd))
    schema_hash = {}
    schema_hash['cols'] = {}
    schema_hash['part_cols'] = {}
    schema_hash['details'] = {}
    schema_hash['storage'] = {}
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
            schema_hash[curr_key][arr[0]] = arr[1]
    return schema_hash


def get_column_list(schema_file):
    schema_text = hdfs_cat(schema_file)
    start_hash = False
    start_col = False
    td_to_hive_type = {
            "INTEGER" : "int",
            "BIGINT" : "bigint",
            "SMALLINT" : "smallint",
            "BYTEINT" : "tinyint",
            "DECIMAL" : "double",
            "REAL" : "double",
            "NUMERIC" : "double",
            "FLOAT" : "float",
            }
    td_type_keys = td_to_hive_type.keys()

    def get_hive_type(td_type):
        for td in td_type_keys:
            if td in td_type:
                return td_to_hive_type[td]
        return "string"

    col_list = []
    for line in schema_text.split('\n'):
        if line.strip() == "(":   #find the opening bracket for column list collection
            start_hash = True
            continue

        if not start_hash:	  #skip lines until start_hash is True
            continue

        if len(line.strip()) > 0 and line.strip()[0]=="(":   #not an empty line, because long columns can contain such a line, and ( ????
            start_col = True
        elif line.find("),")!=-1 and start_col:
            start_col = False
            continue

        if start_col:
            continue

        arr = re.split(r'\s+', line.strip())

        if len(arr) < 2:
            continue

        if line[0:3] == '   ':		#rely on manifest file output starts with 3 spaces, is that a safe assumption?
            if arr[0].strip('"') in hive_keywords:
                arr[0]='`'+arr[0].strip('"')+'`'
        if arr[0].strip().startswith('_'):
            arr[0]=arr[0].lstrip('_')+'_'
        if arr[0].lower() in ('primary', 'unique', 'constraint'):
            break
        col_list.append((arr[0].lower().replace('"',''), get_hive_type(arr[1])))

        if line[-1] == ")":		#end of column def collection
            break
    return col_list


def get_target_schema():
    if args['--hive-only']:
        args['hive_col_list'] = get_column_list(get('hdfs_final_manifest'))
        args['hive_schema'] = ",".join([ "%s %s"%item for item in args['hive_col_list']])
    else:
        args['hive_col_list'] = get_column_list(get('hdfs_manifest'))
        args['hive_schema'] = ",".join([ "%s %s"%item for item in args['hive_col_list']])
    return args['hive_schema']



def create_exec_copy_script():
    refresh = 'FULL' if get_inc_key() is None else 'INC'
    queryid = int(round(time.time() * 1000))
    nummaps = int(calc_num_mappers())
    logging.info("Creating Copy Script %s" % get('copy_file_name'))
    logging.info("Teradata Query ID: %s" % queryid)
    fo = open(get('copy_file_name'), "wb")
    fo.write("export HIVE_HOME=%s\n\n" % get_hive_home())
    fo.write("export HADOOP_CLASSPATH=%s\n\n" % get_hadoop_classpath())
    fo.write("export TDCH_JAR=%s\n\n" % get_tdch_jar())
    fo.write("export LIB_JARS=%s\n\n" % get_lib_jars())
    fo.write("yarn jar $TDCH_JAR %s " % (get('tool_name')))
    fo.write("-Dmapreduce.job.queuename=%s " % (get('queue')))
    fo.write("%s " % (get('compress_string')))
    fo.write("-libjars $LIB_JARS ")
    fo.write("-classname com.teradata.jdbc.TeraDriver ")
    fo.write("-url jdbc:teradata://tdwc/DATABASE=%s  " % (get('Tschema')))
    fo.write("-username %s " % (get('user')))
    fo.write("-password %s " % (get('pwd')))
    fo.write("-accesslock true ")
    fo.write("-queryband 'TableName=%s.%s;QueryId=%s;RefreshType=%s;SplitBy=%s;Size=%s;NumMappers=%s;' " % (get('Tschema'), get('Ttable'), queryid, refresh, get('method'), args['size'], nummaps))
    fo.write("-jobtype %s " % (get('--jobtype')))
    fo.write("-fileformat %s " % (get('--fileformat')))
    fo.write("-method %s " % (get('method')))
    if get('method')=='split.by.hash' and get('--splitByColumn') != None: fo.write("-splitbycolumn %s " % (get('--splitByColumn')))
    fo.write("-nummappers %s " % nummaps)
    fo.write("-targetpaths %s " % (get('hdfsfolder_tmp_data')))
    fo.write("-sourcetable %s " % (get('Ttable')))
    fo.write("%s -targettableschema '%s' \n" % (get('sourceconditions'), get_target_schema()))

    fo.flush()
    fo.close()
    logging.info("Running copy script file %s" % get('copy_file_name'))
    cmd = [which('sh'), get('copy_file_name')]
    (result, stdout, stderr) = execute_command(cmd)
    if result is not 0:
        raise Exception("Process ended abnormally: [%s]" % stderr)
    output = stdout + "\n" + stderr
    for line in output.splitlines():
        if "Map output records=" in line:
            hive_count = int(line.split('=')[1].strip())
            args['hive_count'] = hive_count
    return result, stdout, stderr


def hive_query(query):
    cmd = [get_hive(), "-e",  '"%s"'%query]
    iterations = 3

    for i in range(0, iterations):
        logging.info("Running "+" ".join(cmd))
        (result, stdout, stderr) = execute_command(cmd)
        if result != 0:
            time.sleep(15)
        else:
            return result

    logging.info("...Sending email unable to execute HIVE")
    send_email('HIVE ERROR', 'unable to execute HQL repeatedly %s \n STDOUT: %s \n STDERR: %s ' % (query, stdout, stderr))
    raise Exception("Process ended abnormally: [%s]" % stderr)


def send_email(subject,message):
    msg = MIMEText(message) # Create a text/plain message

    sender = 'edw-infra@groupon.com'
    #recipients = ['edw-infra@groupon.com', 'edw-dev-ops@groupon.com', 'aguyyala@groupon.com', 'kbohra@groupon.com']
    recipients = ['aguyyala@groupon.com']
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To']   = ", ".join(recipients)

    s = smtplib.SMTP('localhost')
    s.sendmail(sender,recipients,msg.as_string())
    s.quit()


def is_new_ddl():
    if 'cols' in args['schema_hash']:
        col_list_hive = args['schema_hash']['cols']
        if 'col_name' in col_list_hive:
            del col_list_hive['col_name']

    col_list_manifest = {key.strip('`'): item.strip() for key, item in dict(args['hive_col_list']).items()}

    if cmp(col_list_hive, col_list_manifest) == 0:
        return
    else:
        if cmp(col_list_hive, col_list_manifest) == -1:
            logging.info("Missing from hive %s" % str(col_list_hive))
            return set(col_list_manifest) - set(col_list_hive)
        else:
            logging.info("Missing from manifest %s" % str(col_list_manifest))
            return set(col_list_hive) - set(col_list_manifest)


def create_hive_ddl():
    hschema = get_hive_schema()
    htable = get_hive_table()
    cols = args['hive_schema']
    loc = get('hdfsfolder_data')
    base_loc = get('base_hdfs')
    part = get('trim_inc_val')
    nn = get_namenode()
    if get_inc_key() is None:
        sql = """use {hschema};
                 DROP TABLE if exists {htable};
                 CREATE EXTERNAL TABLE IF NOT EXISTS {htable}
                 ({cols})
                 stored as orc
                 location '{loc}';""".format(**locals())
    else:
        sql = """use {hschema};
                 CREATE EXTERNAL TABLE IF NOT EXISTS {htable}
                 ({cols})
                 partitioned by (ds string)
                 stored as orc
                 location '{nn}/tmp/replicator/';
                 ALTER TABLE {htable} ADD IF NOT EXISTS PARTITION (ds='{part}');
                 ALTER TABLE {htable} PARTITION (ds='{part}') SET LOCATION '{loc}';
                 """.format(**locals())
    hive_query(sql)


def exec_hive_part():
    args['schema_hash'] = table_schema()
    if not args['schema_hash']['details']:
        create_hive_ddl()
    else:
        result = is_new_ddl()
        if result:
            if get('trim_inc_val') == "D":
                create_hive_ddl()
            else:
                send_email('HIVE DDL mismatch ' + get_hive_schema() + '.' + get_hive_table(),
                           'Manifest columns\n'+str(args['hive_col_list']) +'\nHive columns\n' + str(args['schema_hash'])+ '\nDiff\n' + str(result));
        alter_hive_ddl()
    create_done_file()



def cleanup():
    logging.info("Debug not detected cleaning up temporary files")
    try:
        os.remove(get('script_file_name'))
        os.remove(get('local_manifest'))
        os.remove(get('copy_file_name'))
    except OSError:
        pass


def start_archival():
        if args['import']:
            if not args['--hive-only']:
                create_manifest()
                upload_manifest()
                create_exec_copy_script()
                move_data()
                data_quality()
            else:
                get_target_schema()
                exec_hive_part()
            if not conf['--debug']:
                cleanup()


if __name__ == '__main__':
    args = docopt(__doc__)
    conf = load_conf(args)
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
        start_archival()
    except Exception as e:
        logging.error('ERROR OCCURRED: [%s]' % str(e))
        sys.exit(1)