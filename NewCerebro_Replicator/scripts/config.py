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

import os
import datetime

hive_keywords = ['comment', 'bucket', 'location']

global args


def which(program):
    """
    :param program
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


def get_args(key, default=None):
    val = args.get(key)
    if val is not None:
        return val
    return default


def get_work_dir():
    home_dir = os.path.expanduser('~')
    work_dir = os.path.join(home_dir, 'exportlogs')
    f_work_dir = get_args('--work_dir', work_dir)
    if not os.path.exists(f_work_dir):
        os.makedirs(f_work_dir)
    return f_work_dir


def get_source():
    return get_args('--source', 'tdwc/B_TDHADOOPMOVER,th6jwzhq60')


def get_namenode():
    return get_args('--namenode')


def get_prefix():
    return get_args('--prefix')


def get_schema():
    return get_args('--table').split('.')[0]


def get_table():
    return get_args('--table').split('.')[1]


def get_schema_table():
    return get_args('--table')


def get_hive_schema_table():
    return get_args('--hive_table', get_schema_table()).replace('$','')


def get_hive_schema():
    return get_args('--hive_table', get_schema_table()).split('.')[0]


def get_hive_table():
    return get_args('--hive_table', get_schema_table()).split('.')[1].replace('$','')


def get_inc_key():
    return get_args('--inc_key')


def get_inc_val():
    return get_args('--inc_val')


def get_trimmed_inc_val():
    if get_inc_val() is not None:
        return get_args('--inc_val').replace("-", "")
    else:
        return get_inc_val()


def get_num_mappers():
    return get_args('--nummappers', 0)


def get_default_mappers():
    return 65


def get_threshold_maps():
    return 65


def get_queue():
    return get_args('--queue')


def get_file_format():
    return get_args('--fileformat')


def get_job_type():
    return get_args('--jobtype')


def get_compress_codec():
    return get_args('--compress_codec')


def get_compress():
    return get_args('--compress')


def is_exe(fpath):
    return os.path.exists(fpath) and os.access(fpath, os.X_OK)


def get_hadoop():
    return which("hadoop")


def get_hive():
    return which("hive")


def get(arg):
    return args.get(arg, '')


def get_hive_home():
    return get_args('--HIVE_HOME')


def get_hadoop_classpath():
    return get_args('--HADOOP_CLASSPATH')


def get_tdch_jar():
    return get_args('--TDCH_JAR')


def get_lib_jars():
    return get_args('--LIB_JARS')


def get_tool_name():
    if get_args('import'):
        return get('import_tool')
    else:
        return get('export_tool')


def get_compress_string():
    compress_string = ''
    if get_compress() == "True":
        compress_string = "-Dmapreduce.map.output.compress=%s -Dmapred.map.output.compress.codec=%s" % (str.lower(get_compress()), get_compress_codec())
    return compress_string


def get_method():
    if get_args('--method') is not None:
        return get_args('--method')
    else:
        if get_inc_key() is not None:
            return "split.by.hash"
        else:
            return "split.by.amp"


def load_conf(arg):
    global args
    args = {}
    args.update(arg)
    args['source'] = get_source()
    args['db'] = get_source().split("/")[0]
    args['user'] = get_source().split("/")[1].split(",")[0]
    args['pwd'] = get_source().split("/")[1].split(",")[1]
    args['prefix'] = get_prefix()
    args['namenode'] = get_namenode()
    args['g_backup_dir'] = "%(namenode)s%(prefix)s" % args
    args['g_done_dir'] = args['g_backup_dir'] + "/done"
    args['Tschema'] = get_schema()
    args['Ttable'] = get_table()
    args['Hschema'] = get_hive_schema()
    args['Htable'] = get_hive_table()
    args['inc_key'] = get_inc_key()
    args['inc_val'] = get_inc_val()
    args['trim_inc_val'] = get_trimmed_inc_val()
    args['currentdate'] = datetime.datetime.today().strftime('%Y%m%d')
    args['base_hdfs'] = "%(namenode)s%(prefix)s/%(Hschema)s.%(Htable)s" % args
    args['base_hdfs_tmp'] = "%(namenode)s%(prefix)s/tmp/%(Hschema)s.%(Htable)s" % args

    if get_inc_key() is not None:
        args['hdfsfolder'] = "%(base_hdfs)s/%(trim_inc_val)s" % args
        args['hdfsfolder_tmp'] = "%(base_hdfs_tmp)s/%(trim_inc_val)s" % args
        args['hdfsfile'] = "%(Hschema)s.%(Htable)s" % args
        args['conditions'] = "%s = '%s'" % (get_inc_key(), get_inc_val())
        args['where'] = "where %(conditions)s" % args
        args['sourceconditions'] = "-sourceconditions \"%(conditions)s\"" % args
        args['hdfs_done_dir'] = "%(g_done_dir)s/%(Hschema)s.%(Htable)s/%(trim_inc_val)s/manifest" % args
        args['hdfs_done_file'] = "%(hdfs_done_dir)s/%(Hschema)s.%(Htable)s.manifest.done" % args
    else:
        args['hdfsfolder'] = "%(base_hdfs)s/%(currentdate)s" % args
        args['hdfsfolder_tmp'] = "%(base_hdfs_tmp)s/%(currentdate)s" % args
        args['hdfsfile'] = "%(Hschema)s.%(Htable)s.D" % args
        args['trim_inc_val'] = "D"
        args['hdfs_done_dir'] = "%(g_done_dir)s/%(Hschema)s.%(Htable)s/%(currentdate)s/manifest" % args
        args['hdfs_done_file'] = "%(hdfs_done_dir)s/%(Hschema)s.%(Htable)s.D.manifest.done" % args

    args['method'] = get_method()
    args['schematable'] = "%(Hschema)s_%(Htable)s_%(trim_inc_val)s_%(currentdate)s" % args
    args['final_work_dir'] = get_work_dir()
    args['basefile'] = "%(final_work_dir)s/%(schematable)s" % args
    args['script_file_name'] = "%(basefile)s.sh" % args
    args['copy_file_name'] = "%(basefile)s_copy.sh" % args
    args['local_manifest'] = "%(basefile)s.manifest" % args
    args['hdfsfolder_tmp_manifest'] = "%(hdfsfolder_tmp)s/manifest" % args
    args['hdfsfolder_tmp_data'] = "%(hdfsfolder_tmp)s/data" % args
    args['hdfsfolder_data'] = "%(hdfsfolder)s/data" % args
    args['hdfs_manifest'] = "%(hdfsfolder_tmp_manifest)s/%(hdfsfile)s.manifest" % args
    args['hdfs_final_manifest'] = "%(hdfsfolder)s/manifest/%(hdfsfile)s.manifest" % args

    args['import_tool'] = "com.teradata.connector.common.tool.ConnectorImportTool"
    args['export_tool'] = "com.teradata.connector.common.tool.ConnectorExportTool"
    args['queue'] = get_queue()
    args['compress'] = get_compress()
    args['compress_codec'] = get_compress_codec()
    args['compress_string'] = get_compress_string()
    args['tool_name'] = get_tool_name()

    return args
