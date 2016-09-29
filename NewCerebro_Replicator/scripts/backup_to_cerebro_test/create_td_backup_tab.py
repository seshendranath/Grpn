#!/usr/bin/env python

"""
Usage:
  create_td_backup_tab.py [--backup_dir=<backup_dir>] [--done_dir=<done_dir>] [--table_name=<schema.table>] [--part=<yyyymmdd>] [-v|-vv]
  create_td_backup_tab.py  (-h|--help)

Options:
    --table_name=<schema.table> dir to which teradata data backed up.
    --part=<yyyymmdd> dir to which teradata data backed up.
    --backup_dir=<backup_dir> dir to which teradata data backed up.
    --done_dir=<done_dir> dir to dir where backup done files are created.
"""
from collections import Counter, namedtuple
from docopt import docopt
from email.mime.text import MIMEText

import logging
import os
import re
import sys
import yaml
import tempfile
import datetime
import subprocess
import shlex
import time
import smtplib

g_backup_dir = "" 
g_done_dir = "" 
BACKUP_SCHEMA = "td_backup";
hive_keywords=['comment','bucket','location']

def is_exe(fpath):
    return os.path.exists(fpath) and os.access(fpath, os.X_OK)


def get_hadoop():
    return which("hadoop")

def get_hive():
    return which("hive")

# http://stackoverflow.com/a/377028/625497
def which(program):
    """
    Checks for existence of program on command line and runs full path if it
    exists, None if it does not.
    """
    fpath, fname = os.path.split(program)
    if fpath:
        if is_exe(program):
            return program
    else:
        for path in os.environ["PATH"].split(os.pathsep):
            exe_file = os.path.join(path, program)
            if is_exe(exe_file):
                return exe_file

    raise Exception('%s could not be resolved to an executable' % program)

def execute_command(command, stdout=None, stderr=None, stdin=None,
                    shell=False):
    """
    Invokes command (passed as an array with its arguments) as a subprocess.
    Blocks until command completes. Returns (process result, stdout, stderr).
    """

    p = execute_command_fork(command, stdout, stderr, stdin, shell)
    (stdout, stderr) = p.communicate()
    result = p.returncode

    if stdout is not None: stdout = stdout.strip()
    if stderr is not None: stderr = stderr.strip()

    return (result, stdout, stderr)

def execute_command_fork(command, stdout=None, stderr=None, stdin=None,
                         shell=False):
    """
    Invokes command (passed as an array with its arguments) as a subprocess.

    Does not wait for output. Returns process object.
    """

    if shell:
        command = ' '.join(command)

    if stdout is None: stdout = subprocess.PIPE
    if stderr is None: stderr = subprocess.PIPE

    p = subprocess.Popen(command, stdout=stdout, stderr=stderr, stdin=stdin,
                         shell=shell, bufsize=-1)
    return p



def hdfs_mkdir(path):
    """Executes 'hadoop fs -ls [path]' and returns list of fully-qualified
    subpaths"""
    cmd = [get_hadoop(), "fs", '-mkdir',   path]
    logging.info("Running "+" ".join(cmd))
    (result, stdout, stderr) = execute_command(cmd)
    if result is not 0:
        return False
    return True

def hdfs_touch(path):
    """Executes 'hadoop fs -ls [path]' and returns list of fully-qualified
    subpaths"""
    mkdir_path=path[:path.rfind('/')]
    hdfs_mkdir(mkdir_path)
    cmd = [get_hadoop(), "fs", '-touchz',   path]
    logging.info("Running "+" ".join(cmd))
    (result, stdout, stderr) = execute_command(cmd)
    if result is not 0:
        return False
    return True

def hdfs_test(path):
    """Executes 'hadoop fs -ls [path]' and returns list of fully-qualified
    subpaths"""
    cmd = [get_hadoop(), "fs", '-test', '-e',  path]
    logging.info("Running "+" ".join(cmd))
    (result, stdout, stderr) = execute_command(cmd)
    if result is not 0:
        return False
    return True

def hdfs_ls(path):
    """Executes 'hadoop fs -ls [path]' and returns list of fully-qualified
    subpaths"""
    lst = []
    cmd = [get_hadoop(), "fs", '-ls', path]
    logging.info("Running "+" ".join(cmd))
    (result, stdout, stderr) = execute_command(cmd)
    if result is not 0:
        logging.error("File not found "+stderr)
        return lst

    for line in stdout.strip().split("\n"):
        line = line.strip()
        if "Found " in line:
            continue
        if not line:
            continue
        lst.append(line.split(" ")[-1])
    return lst

def hdfs_cat(path):
    """Executes 'hadoop fs -cat [path]' and returns list of fully-qualified
    subpaths"""
    cmd = [get_hadoop(), "fs", '-cat', path]
    logging.info("Running "+" ".join(cmd))
    (result, stdout, stderr) = execute_command(cmd)
    if result is not 0:
        raise Exception("Process ended abnormally: [%s]" % stderr)

    return stdout.strip()

def hive_query(query):
    cmd = [get_hive(), "-e",  '"%s"'%query]
    iterations = 4

    for i in range(1,iterations):
          logging.info("Running "+" ".join(cmd))
          (result, stdout, stderr) = execute_command(cmd)
          if result != 0:
             time.sleep(8)
          else:
             return result

    print '...Sending email unable to execute HIVE'
    send_email('HIVE ERROR', 'unable to execute HQL repeatedly ' + query)
    raise Exception("Process ended abnormally: [%s]" % stderr)

    
def table_schema(schema , table_name):
    """ executes hive describe <table_name>'. Returns results"""
    cmd = [get_hive(), "-e",  '"use %s;describe formatted %s"'%(schema, table_name)]
    logging.info("Running "+" ".join(cmd))
    schema_hash = {}
    schema_hash['cols'] = {}
    schema_hash['part_cols'] = {}
    schema_hash['details'] = {}
    schema_hash['storage'] = {}
    (result, stdout, stderr) = execute_command(cmd)
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
        arr = shlex.split(line)
        if len(arr) >= 2:
            schema_hash[curr_key][arr[0]] = arr[1]
        
    return schema_hash

def table_partitions(table_name):
    """Executes 'hive show partition <table_name>'. Returns the results
    (result code, stdout, stderr) directly as a 3-tuple."""

    cmd = [get_hive(), "-e",  "show partitions `%s`"% table_name]
    (result, stdout, stderr) = execute_command(cmd)
    if result != 0:
        raise Exception("Process ended abnormally: stdout[%s] stderr[%s] " % (stdout,stderr) )
    return [ line.strip() for line in stdout.split('\n') if line.strip() ]

def get_args(args, key, default):
    val = args.get(key)
    if val is not None:
        return val
    return default

def get_last_file(path):
    lst = hdfs_ls(path) 
    if len(lst) == 0:
        return None
    return lst[-1]

def get_manifest(part):
    manifest_path = os.path.join(part, "manifest")
    return get_last_file(manifest_path)

def get_data(part):
    data_path = os.path.join(part, "lzo")
    return get_last_file(data_path)

def is_full_bkp(schema_file):
    ff = os.path.basename(schema_file)
    lst = ('.D', '.W.', '.M')
    return any(i in ff for i in lst)

def get_column_list(schema_file):
    """ get hive column names and types from td schema file """
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
    col_hash = {}
    td_type_keys = td_to_hive_type.keys()

    def get_hive_type(td_type):
        for td in td_type_keys:
            if td in td_type:
                return td_to_hive_type[td]
        return "string"
        
    col_list = []
    for line in schema_text.split('\n'):
        strip_line = line.strip()
        if strip_line == "(":
            start_hash = True
            continue

        if not start_hash:
            continue

        if len(line.strip()) > 0 and line.strip()[0]=="(":
            start_col=True
        elif line.find("),")!=-1 and start_col:
            start_col=False
            continue
       
        if start_col:
            continue

        #arr = shlex.split(line)
        arr = re.split(r'\s+', line.strip())

        if len(arr) < 2:
            continue

        if line[0:3] == '   ':
            if arr[0].strip('"') in hive_keywords:
	         arr[0]='`'+arr[0].strip('"')+'`'
	    if arr[0].strip().startswith('_'):
	         arr[0]=arr[0].lstrip('_')+'_'
	    col_list.append((arr[0].strip('"').lower(), get_hive_type(arr[1])))

        if line[-1] == ")":
            break

    return col_list

def send_email(subject,message):
        # Create a text/plain message
        msg = MIMEText(message)

	sender = 'edw-infra@groupon.com'
        recipients = ['aguyyala@groupon.com']

        msg['Subject'] = subject
        msg['From'] = sender
        msg['To']   = ", ".join(recipients)

        s = smtplib.SMTP('localhost')
        s.sendmail(sender,recipients,msg.as_string())
        s.quit()

def is_new_ddl(schema_name, table_name, col_list):
    tmp = table_schema(schema_name,table_name)
    if 'cols' in tmp:
        col_list_hive = tmp['cols']
        if 'col_name' in col_list_hive:
           del col_list_hive['col_name'];

    if cmp(col_list_hive,dict(col_list)) == 0:
        return False
    else:
        return True


def create_full_tab(schema_name, table_name, col_list, data_file):
    """ create schema for dimension table pointing to the latest backup"""

    cols = ",".join([ "%s %s"%item for item in col_list ])
    table_name = table_name.translate(None, '!@#$')
    loc = os.path.dirname(data_file)

    if ( is_new_ddl(schema_name, table_name, col_list) ):
      sql = """
            add jar /usr/local/lib/hive/lib/hive-teradata-fast-export-serde-1.0-SNAPSHOT.jar;
            DROP TABLE if exists {schema_name}.{table_name};
            CREATE external table IF NOT EXISTS {schema_name}.{table_name}
            ({cols})
            ROW FORMAT
                SERDE 'com.groupon.hive.serde.TDFastExportSerDe'
                stored as
                        inputformat 'com.groupon.hive.serde.TDFastExportInputFormat'
                        outputformat 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
            location '{loc}'""".format(**locals())
    else:
     sql = """
            add jar /usr/local/lib/hive/lib/hive-teradata-fast-export-serde-1.0-SNAPSHOT.jar;
            use {schema_name};
            ALTER table  {table_name}  SET location '{loc}'""".format(**locals())

    hive_query(sql)

def create_part_tab(schema_name, table_name, col_list):
    """ create table for fact tables, if it does not exists """
    table_name = table_name.translate(None, '!@#$')
    cols = ",".join([ "%s %s"%item for item in col_list ])

    sql = """
            add jar /usr/local/lib/hive/lib/hive-teradata-fast-export-serde-1.0-SNAPSHOT.jar;
            use {schema_name};
            CREATE external table if not exists {table_name}
            ({cols}) partitioned by (ds string)
            ROW FORMAT
                SERDE 'com.groupon.hive.serde.TDFastExportSerDe'
                stored as
                        inputformat 'com.groupon.hive.serde.TDFastExportInputFormat'
                        outputformat 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
            location '/tmp'""".format(**locals())
    hive_query(sql)

def alter_part_tab(schema_name, table_name, schema_hash, hive_col_list):
    """ add new columns to the fact table if the schema has changed"""

    org_cols = schema_hash['cols']
    new_col_list = []
    for name,type in hive_col_list:
        if org_cols.has_key(name):
            continue
        new_col_list.append((name,type))
    if not new_col_list:
        return

    cols = ",".join([ "%s %s"%item for item in new_col_list])

    sql = """
            add jar /usr/local/lib/hive/lib/hive-teradata-fast-export-serde-1.0-SNAPSHOT.jar;
            use {schema_name};
            alter table {table_name} add columns({cols})
        """.format(**locals())
    hive_query(sql)
    

def add_partition(schema_name, table_name, part_list):        
    """ Add new partitions for fact tables """
    full_name = schema_name+"."+table_name
    tab_parts = table_partitions(full_name)
    #logging.info("table parts "+`tab_parts`)
    tab_parts.sort()
    last_tab_part = None
    tab_part_set = set([])
    for tp in tab_parts:
        tab_part_set.add(tp.split('=')[1])
    if tab_parts:
        last_tab_part = tab_parts[-1].split('=')[1]
    add_part_list = []
    for part in part_list:
        ds = os.path.basename(part)            
        #if ds in tab_part_set:
        #    continue

	sql = """
                ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION (ds='{ds}');
                ALTER TABLE {table_name} PARTITION (ds='{ds}') SET LOCATION '{part}/lzo';
                ALTER TABLE {table_name} PARTITION (ds='{ds}') SET SERDE 'com.groupon.hive.serde.TDFastExportSerDe';
                ALTER TABLE {table_name} PARTITION (ds='{ds}') SET FILEFORMAT inputformat 'com.groupon.hive.serde.TDFastExportInputFormat' outputformat 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
                """.format(**locals())
        add_part_list.append(sql);


    if not add_part_list:
        return

    for i in range(1,100):
        range_list =add_part_list[(i-1)*100:i*100]
        if not range_list:
            break;
        add_part_str = ";".join(range_list)
        sql = "add jar /usr/local/lib/hive/lib/hive-teradata-fast-export-serde-1.0-SNAPSHOT.jar;use {schema_name}; {add_part_str}".format(**locals())
        hive_query(sql)

    sql = """
            set hive.mapred.mode=non-strict;
            add jar /usr/local/lib/hive/lib/hive-teradata-fast-export-serde-1.0-SNAPSHOT.jar;
            use {schema_name};
            select * from {table_name} where ds='{ds}' limit 1
        """.format(**locals())
    #hive_query(sql)


def rename_current_tab(schema_name, table_name, schema_hash,col_list):
    """
        move the current dimension table td_backup_archive schema
    """

    loc = schema_hash['details']['Location:']
    cols = ",".join([ "%s %s"%item for item in col_list ])
    date_id = os.path.basename(os.path.dirname(loc))
    archive_schema = 'td_backup_archive'
    sql = """
            add jar /usr/local/lib/hive/lib/hive-teradata-fast-export-serde-1.0-SNAPSHOT.jar;
            use {schema_name};
            drop table if exists {table_name};
            use {archive_schema};
            CREATE external table if not exists {table_name}_{date_id}
            ({cols})
            ROW FORMAT
                SERDE 'com.groupon.hive.serde.TDFastExportSerDe'
                stored as
                        inputformat 'com.groupon.hive.serde.TDFastExportInputFormat'
                        outputformat 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
            location '{loc}'
            """.format(**locals())
            
    hive_query(sql)
            
    
def create_backup_tables(args):
    global g_backup_dir
    global g_done_dir

    g_backup_dir = get_args(args, "--backup_dir", g_backup_dir)
    g_done_dir = get_args(args, "--done_dir", g_done_dir)
    table_name = get_args(args, "--table_name", "")
    part = get_args(args, "--part", "")
    
    table_list = []
    if table_name:
        table_list.append(g_backup_dir+"/"+table_name)
    else:
        table_list = hdfs_ls(g_backup_dir)

    full_bkp = False
    logging.info("Num tables %d", len(table_list))

    for tab in table_list:
        arr = os.path.basename(tab).split('.')
        if len(arr) < 2:
            logging.error("No schema for "+tab)
            continue
    
        (schema_name, table_name) = arr
    
        logging.info("Processing "+tab);
        part_list = []
        if part:
            part_list = [ tab+"/"+part ]
        else:
            part_list = hdfs_ls(tab)
        logging.info("Num parts %d", len(part_list))
        if len(part_list) == 0:
            logging.error("No partitions for "+tab);
            continue
        part_list.sort()

        latest_part = part_list[-1]

        logging.info("lastest part "+latest_part)

        schema_file = get_manifest(latest_part)

        if not schema_file:
             logging.info("No schema file present for "+tab);
             continue

        done_file = schema_file.replace(g_backup_dir, g_done_dir)+".done"
        #tab_create_done = hdfs_test(done_file)

        #if tab_create_done:
        #    logging.info("table creation done file already exists for "+tab)
        #    continue

        data_file = get_data(latest_part)

        if not data_file:
            logging.info("No data file present for "+tab);
            continue

        full_bkp = is_full_bkp(schema_file)
        
        schema_hash = table_schema(BACKUP_SCHEMA, table_name)

        hive_col_list = get_column_list(schema_file)
        logging.info("Schema hash "+`hive_col_list`)
           
        logging.info("full_bkp="+str(full_bkp))
        if full_bkp:    # handle dimension table here
            if schema_hash['details']:
                logging.info("Schema hash "+`schema_hash`)
                #we dont want to keep full table DDL in archive database anymore, no real use case for it 
		#rename_current_tab(BACKUP_SCHEMA, table_name, schema_hash, hive_col_list)

	    create_full_tab(BACKUP_SCHEMA, table_name, hive_col_list, data_file)
            create_full_tab(schema_name, table_name, hive_col_list, data_file)
        else:		 # handle fact tables here
	    if not schema_hash['details'] or schema_hash['details'] :
                create_part_tab(BACKUP_SCHEMA, table_name, hive_col_list)
		create_part_tab(schema_name, table_name, hive_col_list)
            else:       
                logging.info("Schema hash "+`schema_hash`)
		alter_part_tab(BACKUP_SCHEMA, table_name, schema_hash, hive_col_list)
                alter_part_tab(schema_name, table_name, schema_hash, hive_col_list)
	    
            add_partition(BACKUP_SCHEMA, table_name, part_list)	
            add_partition(schema_name, table_name, part_list)
       
        print "--------",done_file
        hdfs_touch(done_file)

if __name__ == '__main__':
    args = docopt(__doc__)
    global dryrun

    level_map = {0: logging.WARNING, 1: logging.INFO, 2: logging.DEBUG}
    format = '%(asctime)s %(levelname)s [%(name)s:%(lineno)d] - %(message)s'
    logging.basicConfig(level=level_map[args['-v']], format=format)
    logging.debug('Args [%s]' % args)

    create_backup_tables(args)

    

