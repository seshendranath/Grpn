#!/usr/bin/env python

"""
Usage:
  RunReplicator.py (import | export) --table=<schema>.<table> [--view] [--source=<db>/<username>.<password>]
                   [--namenode=<namenode>] [--prefix=<prefix>] [--hive_table=<hschema>.<htable>] [--jobtype=<jobtype>]
                   [(--inc_key=<inc_key> --inc_val=<inc_val>)] [--work_dir=<work_dir>] [--queue=<queue>] [--compress=<boolean>]
                   [--compress_codec=<codec>] [--nummappers=<no>] [--fileformat=<fileformat>] [--debug] [--HIVE_HOME=<$Hive_Home>]
                   [--method=<method>] [--splitByColumn=<primary_index>] [--HADOOP_CLASSPATH=<$HADOOP_CLASSPATH>] [--TDCH_JAR=<TDCHjarpath>] [--LIB_JARS=<HiveLibJarsPath>]
                   [--hive-only] [-v|-vv]
  RunReplicator.py (-h | --help)

Options:
  import                                            Import data from Teradata to Hadoop
  export                                            Export data from Hadoop to Teradata
  --table=<schema>.<table>                          Specify Teradata Table to Import to hadoop
  --view                                            Set this flag if the Teradata object is view
  --source=<db>/<username>.<password>               Specify source (tdwc\tdwb), username, and pwd
  --namenode=<namenode>                             Specify Hadoop cluster Namenode to copy [default: hdfs://cerebro-namenode]
  --prefix=<prefix>                                 Specify under which directory to copy in HDFS [default: /user/grp_edw_rep_prod/backup]
  --hive_table=<hschema>.<htable>                   Users can specify different hive table name to create in hive (default: --table)
  --jobtype=<jobtype>                               Specify job type (ex: hive|hdfs) [default: hive], if specifying hdfs, specify right fileformats (Text)
                                                        and compress to False unless you have alternative to read compressed data
  --inc_key=<inc_key>                               Specify the partitioned column for incremental refresh
  --inc_val=<inc_val>)                              Specify the incremental value for incremental refresh
  --work_dir=<work_dir>                             Local work directory to create temp files (default: ~/exportlogs)
  --queue=<queue>                                   Hadoop Queue Name [default: replicator]
  --compress=<boolean>                              Compress the data or not (ex: True|False) [default: True]
  --compress_codec=<codec>                          Compression codec [default: org.apache.hadoop.io.compress.SnappyCodec]
  --nummappers=<no>                                 No. of mappers to copy (default: will calculate based on prev size)
  --splitByColumn=<primary_index>                   Specify the primary index column on which the data is evenly distributed, this param is valid only for split.by.hash and split.by.value 
  --fileformat=<fileformat>                         File format in HDFS (ex: orcfile|textfile) [default: orcfile]
  --debug                                           Will not cleanup local files
  --HIVE_HOME=<$Hive_Home>                          Search through hive lib [default: /usr/local/lib/hive]
  --method=<method>                                 Specify which method to use (ex: split.by.partition or split.by.amp) default is split.by.amp for full refresh and split.by.hash for incremental
  --HADOOP_CLASSPATH=<$HADOOP_CLASSPATH>            Search through hadoop lib [default: "$HIVE_HOME/conf:$HIVE_HOME/lib/antlr-runtime-3.4.jar:$HIVE_HOME/lib/commons-dbcp-1.4.jar:$HIVE_HOME/lib/commons-pool-1.5.4.jar:$HIVE_HOME/lib/datanucleus-connectionpool-3.2.9.jar:$HIVE_HOME/lib/datanucleus-core-3.2.9.jar:$HIVE_HOME/lib/datanucleus-rdbms-3.2.9.jar:$HIVE_HOME/lib/hive-cli-1.2.1.jar:$HIVE_HOME/lib/hive-exec-1.2.1.jar:$HIVE_HOME/lib/hive-metastore-1.2.1.jar:$HIVE_HOME/lib/jdo-api-3.0.1.jar:$HIVE_HOME/lib/libfb303-0.9.2.jar:$HIVE_HOME/lib/libthrift-0.9.2.jar"]
  --TDCH_JAR=<TDCHjarpath>                          Search through TDCH lib [default: /home/svc_replicator_prod/tdch/1.4/lib/teradata-connector-1.4.2.jar]
  --LIB_JARS=<HiveLibJarsPath>                      Search through hive lib [default: "$HIVE_HOME/lib/hive-cli-1.2.1.jar,$HIVE_HOME/lib/hive-exec-1.2.1.jar,$HIVE_HOME/lib/hive-metastore-1.2.1.jar,$HIVE_HOME/lib/libfb303-0.9.2.jar,$HIVE_HOME/lib/libthrift-0.9.2.jar,$HIVE_HOME/lib/jdo-api-3.0.1.jar"]
  --hive-only                                       Will execute hive part only
  -v|-vv                                            Logging level
"""

from docopt import docopt
from config import *
from decimal import *
from email.mime.text import MIMEText
import subprocess
import tempfile
import select
import logging
import re
import math
import time
import shlex
import smtplib
import collections
import sys


def collectify(item):
    """
    :param item
    Forces any argument into a collection.

    If an item is iterable, return it, else wrap it in an array and return
    it. If the item is None, return an empty array.
    """

    if isinstance(item, collections.Iterable) and not isinstance(item, str):
        return item
    elif item is None:
        return []
    else:
        return [item]

def execute_command(command, stdout=None, stderr=None, stdin=None,
                    shell=False, env=None, do_sideput=True):
    """
    :param command;
    :param stdout
    :param stderr
    :param stdin
    :param shell
    :param env
    :param do_sideput
    Invokes command (passed as an array with its arguments) as a subprocess.
    Blocks until command completes. Returns (process result, stdout, stderr).
    """

    if do_sideput:
        (result, stdout, stderr) = execute_command_fork_sideput(command, stdout, stderr, stdin, shell, env)
    else:
        p = execute_command_fork(command, stdout, stderr, stdin, shell, env)
        (stdout, stderr) = p.communicate()
        result = p.returncode

    if stdout is not None: stdout = stdout.strip()
    if stderr is not None: stderr = stderr.strip()

    return result, stdout, stderr


def execute_command_fork(command, stdout=None, stderr=None, stdin=None,
                         shell=False, env=None):
    """
    :param command;
    :param stdout
    :param stderr
    :param stdin
    :param shell
    :param env
    Invokes command (passed as an array with its arguments) as a subprocess.

    Does not wait for output. Returns process object.
    """

    command = map(str, collectify(command)) # coerce args to strings
    if shell:
        command = ' '.join(command)

    if stdout is None: stdout = subprocess.PIPE
    if stderr is None: stderr = subprocess.PIPE

    p = subprocess.Popen(command, stdout=stdout, stderr=stderr, stdin=stdin,
                         shell=shell, bufsize=-1, env=env)
    return p


def execute_command_fork_sideput(command, stdout=None, stderr=None, stdin=None,
                         shell=False, env=None):
    """
    :param command;
    :param stdout
    :param stderr
    :param stdin
    :param shell
    :param env
    Invokes command (passed as an array with its arguments) as a subprocess.

    Does not wait for output. Returns process object.
    """

    command = map(str, command) # coerce args to strings
    if shell:
        command = ' '.join(command)



    if stdout is None:
        stdout = tempfile.NamedTemporaryFile(prefix='stdout_', suffix='.log', dir='/tmp')
    if stderr is None:
        stderr = tempfile.NamedTemporaryFile(prefix='stderr_', suffix='.log', dir='/tmp')

    p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=stdin,
                         shell=shell, bufsize=-1, env=env)

    while p.poll() is None:
        reads = [p.stdout.fileno(), p.stderr.fileno()]
        try: ret = select.select(reads, [], [])
        except select.error as ex:
            if ex[0] == 4: continue
            else: raise
        for fd in ret[0]:
            if fd == p.stdout.fileno():
                read = p.stdout.readline()
                logging.info(read.strip("\n"))
                stdout.write(read)
            if fd == p.stderr.fileno():
                read = p.stderr.readline()
                logging.info(read.strip("\n"))
                stderr.write(read)

    p.wait()
    read = p.stdout.read()
    logging.info(read.strip("\n"))
    stdout.write(read)

    read = p.stderr.read()
    logging.info(read.strip("\n"))
    stderr.write(read)


    stdout.seek(0)
    stderr.seek(0)

    result = p.returncode
    stdo = stdout.read()
    stde = stderr.read()

    return result, stdo, stde


def hdfs_mkdir(path):
    """
    :param path
    Executes 'hadoop fs -ls [path]' and returns list of fully-qualified
    subpaths"""
    cmd = [get_hadoop(), "fs", '-mkdir', '-p', path]
    logging.info("Running "+" ".join(cmd))
    (result, stdout, stderr) = execute_command(cmd)
    if result is not 0:
        raise Exception("Process ended abnormally: [%s]" % stderr)
    return True


def hdfs_rm(path):
    cmd = [get_hadoop(), "fs", '-rm', '-r', '-skipTrash',   path]
    logging.info("Running "+" ".join(cmd))
    (result, stdout, stderr) = execute_command(cmd)
    if result is not 0:
        raise Exception("Process ended abnormally: [%s]" % stderr)
    return True


def hdfs_put(local, hdfspath):
    cmd = [get_hadoop(), "fs", '-put', local, hdfspath]
    logging.info("Running "+" ".join(cmd))
    (result, stdout, stderr) = execute_command(cmd)
    if result is not 0:
        raise Exception("Process ended abnormally: [%s]" % stderr)
    return True


def hdfs_mv(source, destination):
    cmd = [get_hadoop(), "fs", '-mv', source, destination]
    logging.info("Running "+" ".join(cmd))
    (result, stdout, stderr) = execute_command(cmd)
    if result is not 0:
        raise Exception("Process ended abnormally: [%s]" % stderr)
    return True


def hdfs_touch(path):
    mkdir_path=path[:path.rfind('/')]
    hdfs_mkdir(mkdir_path)
    cmd = [get_hadoop(), "fs", '-touchz',   path]
    logging.info("Running "+" ".join(cmd))
    (result, stdout, stderr) = execute_command(cmd)
    if result is not 0:
	raise Exception("Process ended abnormally: [%s]" % stderr)
    return True


def hdfs_test(path):
    cmd = [get_hadoop(), "fs", '-test', '-e',  path]
    logging.info("Running "+" ".join(cmd))
    (result, stdout, stderr) = execute_command(cmd)
    if result is not 0:
        return False
    return True


def hdfs_ls(path):
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
    cmd = [get_hadoop(), "fs", '-cat', path]
    logging.info("Running "+" ".join(cmd))
    (result, stdout, stderr) = execute_command(cmd)
    if result is not 0:
        raise Exception("Process ended abnormally: [%s]" % stderr)

    return stdout.strip()


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


def get_prev_size():
    size = 0
    cmd = ["hadoop fs -du %s |  grep -v 'ds=' | tail -1 | awk '{print $1}'" % get('base_hdfs')]
    logging.info("Running "+" ".join(cmd))
    (result, stdout, stderr) = execute_command(cmd, shell=True)
    if result is not 0:
        logging.info("Error while finding size " + stderr)
        return 0

    for line in stdout.strip().split("\n"):
        line = line.strip()
        if not line:
            continue
        size = line
    return size


def calc_num_mappers():
    args['size'] = get_prev_size()
    nummaps = int(math.ceil(Decimal(args['size'])/Decimal((1024*1024*1024)))) * 2
    if get_num_mappers() != 0:
        return get_num_mappers()
    else:
        if int(args['size']) != 0:
            if nummaps < get_threshold_maps():
                return nummaps
            else:
                return get_threshold_maps()

    return get_default_mappers()


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


def move_data():
    if not hdfs_test(get('base_hdfs')):
        logging.info("Base Folder didn't exist. Creating one.")
        hdfs_mkdir(get('base_hdfs'))
    if hdfs_test(get('hdfsfolder')):
        logging.info("Previous Data Folder detected removing it")
        hdfs_rm(get('hdfsfolder'))
    logging.info("Moving data to final location")
    hdfs_mv(get('hdfsfolder_tmp'), get('base_hdfs'))


def data_quality():
    text = hdfs_cat(get('hdfs_final_manifest'))
    args['td_count'] = int(text.splitlines()[1].split('|')[1].strip())
    logging.info(" TD(%s)=Hive(%s)" % (args['td_count'], args['hive_count']))
    if args['td_count'] <= args['hive_count']:
        logging.info("CORRECT")
        exec_hive_part()
        return True
    else:
        logging.info("MISMATCH")
        return False


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


def alter_hive_ddl():
    hschema = get_hive_schema()
    htable = get_hive_table()
    loc = get('hdfsfolder_data')
    part = get('trim_inc_val')
    if get('trim_inc_val') == "D":
        sql = """use {hschema};
                 ALTER TABLE {htable} SET LOCATION '{loc}';
              """.format(**locals())
    else:
        sql = """use {hschema};
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


def create_done_file():
    logging.info("Creating Done File")
    hdfs_touch(get('hdfs_done_file'))


def upload_manifest():
    if hdfs_test(get('hdfsfolder_tmp')):
        logging.info("Previous tmp Folder detected removing it")
        hdfs_rm(get('hdfsfolder_tmp'))
    logging.info("Creating HDFS Temp Folder")
    hdfs_mkdir(get('hdfsfolder_tmp_manifest'))
    logging.info("Uploading Manifest file to hdfs")
    hdfs_put(get('local_manifest'), get('hdfs_manifest'))


def cleanup():
    logging.info("Debug not detected cleaning up temporary files")
    try:
        os.remove(get('script_file_name'))
        os.remove(get('local_manifest'))
        os.remove(get('copy_file_name'))
    except OSError:
        pass


def start_replicator():
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
        start_replicator()
    except Exception as e:
        logging.error('ERROR OCCURRED: [%s]' % str(e))
        sys.exit(1)
