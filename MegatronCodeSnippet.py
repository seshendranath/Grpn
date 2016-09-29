####Code snippet from Megatron

#!/usr/bin/env python

"""
Usage:
  merge_tab.py --tab=<schema.tab> --tungsten_conf=<conf_file> --db_conf=<conf_file> --workflow_id=<run_id> --run_mode=<mode> [--test_mode] [--dryrun] [--force] [--create_schema] [-v|-vv]
  merge_tab.py (-h|--help)

Options:
    dryrun enable dry run
    force is used with sqoop to force sqoop
          or with full load to force a full load
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
from shared  import os_util
from shared  import hdfs
from shared  import hive_util
from shared  import yaml_util
from mysqlInfo import MysqlInfo
from config import *
from util import run_zr, make_dir
from time import gmtime, strftime
from mako.template import exceptions,Template
from etlstatusdb import *
from td_util import *
from sql_adaptor import *
from hiveInfo import HiveInfo, lsrd, run_hive_sql
import MySQLdb
import sys
from collections import namedtuple

g_script_dir = os.path.dirname(os.path.realpath(__file__))
g_smart_counters = None
g_src_max_pkid = 0

HIVE_RESERVED = ( 'true', 'false', 'all', 'and', 'or', 'not', 'like', 'asc', 'desc', 'order', 'by', 'group', 'where',
                  'from', 'as', 'select', 'distinct', 'insert', 'overwrite', 'outer', 'join', 'left', 'right',
                  'full', 'on', 'partition', 'partitions', 'table', 'tables', 'tblproperties', 'show', 'msck',
                  'directory', 'local', 'locks', 'transform', 'using', 'cluster', 'distribute', 'sort', 'union', 'load',
                  'data', 'inpath', 'is', 'null', 'create', 'external', 'alter', 'describe', 'drop', 'reanme', 'to',
                  'comment', 'boolean', 'tinyint', 'smallint', 'int', 'bigint', 'float', 'double', 'date',
                  'datetime', 'timestamp', 'string', 'binary', 'array', 'map', 'reduce', 'partitioned',
                  'clustered', 'sorted', 'into', 'buckets', 'row', 'format', 'delimited', 'fields', 'terminated',
                  'collection', 'items', 'keys', 'lines', 'stored', 'sequencefile', 'textfile', 'inputformat',
                  'outputformat', 'location', 'tablesample', 'bucket', 'out', 'of', 'cast', 'add', 'replace',
                  'columns', 'rlike', 'regexp', 'temporary', 'function', 'explain', 'extended', 'serde', 'with',
                  'serdeproperties', 'limit', 'set', 'tblproperties' )

DELIMITER_HEX_STR = "01"
DELIMITER_HIVE = r"\1"
MAX_TRASH_AGE = 2

class Counter(object):
    def __init__(self, name, run_id, min_pkid, max_pkid):
        self.name = name
        self.run_id = run_id
        self.min_pkid = int(min_pkid)
        self.max_pkid = int(max_pkid)
        if (self.max_pkid - self.min_pkid) > BOUNDED_COUNT_RANGE:
            self.min_pkid = self.max_pkid - BOUNDED_COUNT_RANGE
        if self.min_pkid < 0: self.min_pkid = 0
        if self.max_pkid < 0: self.max_pkid = 0
        self.row_count = 0

    def descriptor(self):
        return "{self.name}:{self.min_pkid},{self.max_pkid}".format(**locals())
        
    def fetch_results(self, hiveInfo, hdfs_temp_dir):
        self.row_count = hiveInfo.get_counter_value(hdfs_temp_dir, self.name) 
    
    def get_source_count(self, source_adaptor):
        return source_adaptor.get_row_count(self.min_pkid, self.max_pkid)
        

class SmartCounters(object):
    def __init__(self):
        self.counters = {}
        self.table_name = hive_tab()
        self.service_name = get_service_name()
        self.process_type = get_run_mode()
        self.src_adaptor = get_source_adaptor()
        
        # Initialize all counters for this run
        etl_db = EtlStatusDb(conf['etl_status_dsn'])
        (run_id, min_pkid, max_pkid) = etl_db.get_last_success_increment_count_task(self.service_name, self.table_name)
        if run_id:
            (min_increment_pkid, max_increment_pkid, min_window_pkid, max_window_pkid) = etl_db.get_integrity_count_ranges(max_pkid)
            self.counters["increment_count"] = Counter("increment_count", run_id, min_increment_pkid, max_increment_pkid)
            self.counters["window_count"] = Counter("window_count", run_id, min_window_pkid, max_window_pkid)
            
            (fail_run_id, min_pkid, max_pkid, cnt_failures) = etl_db.get_recheck_increment_count_task(self.service_name, self.table_name, self.process_type)
            if cnt_failures > 0:
                self.counters["increment_recount"] = Counter("increment_recount", fail_run_id, min_pkid, max_pkid)

            (fail_run_id, min_pkid, max_pkid, cnt_failures) = etl_db.get_recheck_random_window_count_task(self.service_name, self.table_name, self.process_type)
            if cnt_failures > 0:
                self.counters["window_recount"] = Counter("window_recount", fail_run_id, min_pkid, max_pkid)
        else:
            logging.warn("BUG: No successful run found prior to run: %s"%get_run_id())
    
    def get_tasks_descriptor(self):
        cnt_descrs = []
        for counter in self.counters.itervalues():
            cnt_descrs.append(counter.descriptor())
        return '/'.join(cnt_descrs)
        
    def fetch_and_update_all_counter_results(self):
        etl_db = EtlStatusDb(conf['etl_status_dsn'], get_table_limits_dsn() )
        hiveInfo = HiveInfo(get_conf()['hive_schema'])
        hdfs_temp_loc = get_hdfs_temp_loc()
        table_online_state = "SUCCESS"
        (src_window_count, dest_window_count) = (0, 0)
        
        for counter in self.counters.itervalues():
            counter.fetch_results(hiveInfo, hdfs_temp_loc)
        
        if "increment_count" in self.counters:
            (run_id, minid, maxid, src_count, dest_count) = self.get_counter_data("increment_count")
            etl_db.update_increment_counts(run_id, self.service_name, self.table_name, self.process_type, src_count, dest_count)

        if "window_count" in self.counters:
            (run_id, minid, maxid, src_count, dest_count) = self.get_counter_data("window_count")
            success = etl_db.update_window_counts(run_id, self.service_name, self.table_name, self.process_type, minid, src_count, dest_count)

        if "increment_recount" in self.counters:
            (run_id, minid, maxid, src_count, dest_count) = self.get_counter_data("increment_recount")
            success = etl_db.update_increment_counts(run_id, self.service_name, self.table_name, self.process_type, src_count, dest_count)
            if not success:
                table_online_state = "FAILURE:INCREMENTAL_COUNT"

        if "window_recount" in self.counters:
            (run_id, minid, maxid, src_count, dest_count) = self.get_counter_data("window_recount")
            success = etl_db.update_window_counts(run_id, self.service_name, self.table_name, self.process_type, minid, src_count, dest_count)
            if not success:
                table_online_state = "FAILURE:WINDOW_COUNT"
        
        return table_online_state
        
    def get_counter_data(self, counter_name):
        counter = self.counters[counter_name]
        src_row_count = self.src_adaptor.get_row_count(counter.min_pkid, counter.max_pkid)
        return (counter.run_id, counter.min_pkid, counter.max_pkid, src_row_count, counter.row_count)

def tab_name():
    """ get mysql table name """
    return conf['--tab']

def target_table_name():
    """ get mysql table name """
    return conf['table_name']

def td_tab_name():
    tab = target_table_name()
    (target_name, target_dsn, schema, table_prefix) = get_target_info()
    return get_string_shortener().shorten(table_prefix + tab)

def get_target_table_name():
    if get_run_mode() == "load" or get_run_mode() == "full_load":
        tbname = td_tab_name()
    else:
        tbname = target_table_name()
    return tbname

def tab_name_tup():
    """ return schema and table name """
    return tab_name().split(".")

def hive_tab():
    """ return schema_tablename """

    return "_".join(tab_name_tup())

def stg_tab_name(mode=None):
    """ return stg table name """
    if mode is not None:
        return hive_tab()+"_"+mode+"_stg"
    else:
        return hive_tab()+"_"+get_run_mode()+"_stg"

# TBD: Change final table name one without final
def final_tab_name():
    """ return final table name """

    return hive_tab()+"_final"

def table_external_loc(table_name):
    """ return current external location of final table 
    """
    hiveSchema = get_conf()['hive_schema']
    hiveInfo = HiveInfo(hiveSchema)
    schemaMap = hiveInfo.get_tab_schema_detail(table_name)
    return schemaMap['details'].get('location', None)

def table_create_date(table_name):
    """ return table create date 
    """
    hiveSchema = get_conf()['hive_schema']
    hiveInfo = HiveInfo(hiveSchema)
    schemaMap = hiveInfo.get_tab_schema_detail(table_name)
    return schemaMap['details'].get('createtime', None)

def final_tab_external_loc():
    return table_external_loc(final_tab_name())

def sqoop_table_name():
    return hive_tab()+"_sqoop"

def current_table_name():
    return final_tab_name()+"_current"

def diff_table_name():
    return final_tab_name()+"_diff"

def get_stg_tab_loc():
    """ return stg table loc """

    loc = conf['stage_location']
    (schema, tab) = tab_name_tup()
    return "{loc}/{schema}/{tab}".format(**locals())

def get_sqoop_tab_loc(workflow_id=None):
    """ return sqoop table loc """

    loc = conf['final_table_loc']
    (schema, tab) = tab_name_tup()
    service_name = get_service_name()
    if not workflow_id:
        workflow_id = get_run_id()
    return "{loc}/{service_name}/{schema}/{tab}/{schema}_{tab}_{workflow_id}_sqoop".format(**locals())

def check_schema_change(schemaInfo):
    hiveSchema = get_conf()['hive_schema']
    hiveInfo = HiveInfo(hiveSchema)
    tab_cols = hiveInfo.get_table_schema(final_tab_name())
    hive_list = list(zip(*tab_cols)[0])
    
    if cmp(hive_list,schemaInfo.col_list) == 0:
        return False
    else:
        if (cmp(hive_list,schemaInfo.col_list) == -1 ):
                logging.info("Hive Cols: %s Mysql Cols: %s Missing from Hive: %s" , str(hive_list), str(schemaInfo.col_list), set(schemaInfo.col_list)- set(hive_list))
                return True
        else:
                logging.info("Hive Cols: %s Mysql Cols: %s Missing from Mysql: %s"  , str(hive_list), str(schemaInfo.col_list), set(hive_list) - set(schemaInfo.col_list))
                return True


def create_stg_tab(schemaInfo):
    """ create stg table if not exists """

    hive_schema = conf['hive_schema']
    stg_tab = stg_tab_name()
    col_def_str = ",".join([ "`%s` string"%col for col in schemaInfo.col_list ])
    loc = get_stg_tab_loc()

    sql = """use {hive_schema};
              create external table if not exists {stg_tab} (
               tungsten_opcode string, tungsten_seqno bigint, tungsten_rowid bigint, tungsten_time string,
                   {col_def_str} ) partitioned by(ds string, hr string) location '{loc}';
                   """.format(**locals())
    if not conf["--dryrun"]:
        run_hive_sql(sql)

# TBD: Check tungsten schema flags 
def is_sqoop_needed(schemaInfo):
    """ check re-sqoop is needed ,
            if mysql schema has changed
            or first time
            or force option enabled """
    global g_src_max_pkid
    print_hive_info()
    hiveSchema = get_conf()['hive_schema']
    hiveInfo = HiveInfo(hiveSchema)
    tab_cols = hiveInfo.get_table_schema(final_tab_name())

    """ If the source table is empty then the sqoop is not needed"""
    source_adaptor = get_source_adaptor()
    g_src_max_pkid = int(source_adaptor.get_max_pkid())
    logging.info("g_src_max_pkid: %s" % g_src_max_pkid)
    if g_src_max_pkid == 0:
        logging.info("Source Table is Empty")
        return False
    
    if not tab_cols:
        logging.info("Final table does not exists, sqoop needed")
        return True
    else:
        logging.info("Hive table exists: %s  - %s" % (final_tab_name(), hiveSchema))

    if check_schema_change(schemaInfo):
        return True

    if not get_conf()['last_sqoop_time']:
        return True
 
    return False



def print_hive_info():
    hive_schema = conf['hive_schema']
    final_tab = final_tab_name()
    logging.info("SCHEMA: {hive_schema} -> {final_tab}".format(**locals()))


def get_tmp_hive_table_name():
    if get_run_mode() == "merge":
        return current_table_name()
    elif get_run_mode() == "load":
        return diff_table_name()
    elif get_run_mode() == "sqoop":
        return sqoop_table_name()
    return ""

def is_tmp_hive_table_exists():
    """ check if _current table is present then something means it is running """

    hiveInfo = HiveInfo(get_conf()['hive_schema'])
    tab_name = get_tmp_hive_table_name()

    createtime = table_create_date(tab_name)
    if not createtime:
        return False

    return True

def drop_tmp_hive_table():
    hive_schema = conf['hive_schema']
    tab_name = get_tmp_hive_table_name()
    success = True
    if tab_name:
        try:
            sql = "use {hive_schema}; drop table if exists {tab_name} ".format(**locals())
            run_hive_sql(sql)
        except:
            success = False
            pass
    return success

def get_sqoop_tab_sql(schemaInfo):
    """ create the table where sqooped data is loaded """

    hive_schema = conf['hive_schema']
    final_tab = final_tab_name()
    col_def_str = ",".join([ "`%s` string"%col for col in schemaInfo.col_list ])
    loc = get_sqoop_tab_loc()
    delimiter = DELIMITER_HIVE
    tab_name = sqoop_table_name()
    sql = """use {hive_schema};
                  create external table {tab_name} (
                    {col_def_str} ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '{delimiter}' location '{loc}';
                """.format(**locals())

    return sql

def get_rename_sqoop_tab_sql():

    hive_schema = conf['hive_schema']
    final_tab = final_tab_name()
    sqoop_tab_name = sqoop_table_name()
    merge_stg_tab = stg_tab_name("merge")
    load_stg_tab = stg_tab_name("load")
    sql = "use {hive_schema}; drop table if exists {merge_stg_tab}; drop table if exists {load_stg_tab}; drop table if exists {final_tab}; alter table {sqoop_tab_name} rename to {final_tab}".format(**locals())

    return sql

def get_stg_tab_dirs():
    """ find dirs in stg table, where the tungsten data comes """

    from datetime import datetime 
    
    loc = get_stg_tab_loc()
    last_success_time = get_min_part()
    last_proc_time = datetime.strptime(last_success_time,"ds=%Y-%m-%d/hr=%H") if last_success_time != "ds=0000-00-00/hr=00" else datetime(1970, 1, 1, 0, 0, 0)
    dir_list = lsrd(loc, last_proc_time)
    pat = re.compile(".*"+loc+"/ds=(\d{4}-\d{2}-\d{2})/hr=(\d{2})$")
    part_hash = {}
    for dir in dir_list:
        m = pat.match(dir)
        if not m:
            continue
        part_str = "ds=%s/hr=%s"%(m.group(1), m.group(2))
        part_hash[part_str] = (m.group(1), m.group(2))

    return part_hash

def get_stg_tab_parts():
    """ get partition of stg table where tungsten data comes """

    hiveInfo = HiveInfo(get_conf()['hive_schema'])
    return hiveInfo.table_partitions(stg_tab_name())

def get_merge_frequncy():

    merge_run_frequncy_hrs = conf.get('merge_run_frequncy_hrs', 0)
    tab = tab_name()
    merge_run_frequncy_hrs = conf.get('table_info', {}).get(tab, {}).get('merge_run_frequncy_hrs', merge_run_frequncy_hrs)
    return merge_run_frequncy_hrs

def is_ready_to_run(first_part):
    merge_run_frequncy_hrs = get_merge_frequncy()

    import datetime as dt
    dt1 = dt.datetime.strptime(first_part, "ds=%Y-%m-%d/hr=%H")
    td = dt.datetime.now() - dt1
    num_hrs = td.total_seconds()/3600.0
    logging.info("num_hrs=%f merge_run_frequncy_hrs=%f"%( num_hrs, merge_run_frequncy_hrs))
    if num_hrs >= merge_run_frequncy_hrs:
        return True
    logging.info("No need to run wait more time")
    return False

def get_min_part():
    tab_name = hive_tab()
    service_name = get_service_name()
    etl_db = EtlStatusDb(conf['etl_status_dsn'])
    last_sqoop_time = etl_db.get_last_op_time(service_name, tab_name,
                                              'sqoop', 'SUCCESS')
    last_full_load_time = etl_db.get_last_op_time(service_name, tab_name,
                                                  'full_load', 'SUCCESS')
    last_merge_time = etl_db.get_last_op_time(service_name, tab_name,
                                              'merge', 'SUCCESS')
    last_load_time = etl_db.get_last_op_time(service_name, tab_name,
                                             'load', 'SUCCESS')
    from datetime import timedelta
    min_part = "ds=0000-00-00/hr=00"

    last_op_time = last_merge_time

    process_type = get_run_mode()
    if process_type == "load":
        last_op_time = last_load_time

    if (last_sqoop_time and last_op_time and  last_sqoop_time > last_op_time) or (last_sqoop_time and not last_op_time):
        last_sqoop_time = last_sqoop_time - timedelta(hours=2)
        min_part = last_sqoop_time.strftime("ds=%Y-%m-%d/hr=%H")
        logging.info("min part based on sqoop/full_load %s"%min_part)
        return min_part

    min_part = etl_db.get_last_success_part(service_name, tab_name, process_type )

    if not min_part and last_sqoop_time:
        last_sqoop_time = last_sqoop_time - timedelta(hours=2)
        min_part = last_sqoop_time.strftime("ds=%Y-%m-%d/hr=%H")

    return min_part if min_part else "ds=0000-00-00/hr=00"

def gen_add_partition_sql():
    """ find missing partitions that needes to be added """

    dir_hash = get_stg_tab_dirs()

    all_dir_list = sorted(dir_hash.keys())

    logging.info("all dirs %r", all_dir_list)

    add_part_list = all_dir_list

    logging.info("filtered dirs %r", add_part_list)

    loc = get_stg_tab_loc()
    stg_table_name = stg_tab_name()
    sql_list = []
    where_list = []
    for pt in add_part_list:
        (ds, hr) = dir_hash[pt]
        sql = "ALTER TABLE {stg_table_name} ADD IF NOT EXISTS PARTITION (ds='{ds}', hr='{hr}') LOCATION '{loc}/ds={ds}/hr={hr}'".format(**locals())
        where_list.append("(ds='{ds}' and hr='{hr}')".format(**locals()))
        sql_list.append(sql)

    sql_stmt = ";".join(sql_list)

    where_stmt = " or  ".join(where_list) if len(where_list) > 0 else "1 = 0"

    logging.info("merge where %s", where_stmt)
    logging.info("add part sql %s", sql_stmt)

    if sql_stmt:
        hive_schema = conf['hive_schema']
        sql_stmt = "use %s; "%hive_schema+sql_stmt

    return (where_stmt, sql_stmt, add_part_list)

def get_tab_dir():
    run_dir = get_run_dir()
    tab = tab_name()
    tab_dir = os.path.join(run_dir, tab)
    return tab_dir

def make_tab_dir():
    tab_dir = get_tab_dir()
    make_dir(tab_dir)
    return tab_dir

def get_tab_log_file():
    return os.path.join(get_tab_dir(), "%s.log" % tab_name() )

def get_yml_file():
    tab_dir = get_tab_dir()
    tab = tab_name()
    return os.path.join(get_tab_dir(), tab+".yml")

def rmr_task(dep_task, loc):
    return {
        "class" : "HDFSTask",
        "dependencies" : [ dep_task ],
        "configuration" : {
            "work": { "try" : { "rmr" : loc } }}
    }

def get_source_adaptor():
    dsn = conf["dsn"]
    (schema, tab) = tab_name_tup()
    return SQLAdaptor(dsn, schema, tab)

def get_destination_adaptor():
    src_adaptor = get_source_adaptor()
    dest_adaptor = None
    run_mode = get_run_mode()
    
    if run_mode in ["load", "full_load"]:
        (target_name, target_dsn, schema, table_prefix) = get_target_info()
        dest_adaptor = SQLAdaptor(target_dsn, schema, td_tab_name(), src_adaptor.schemaInfo)
    elif run_mode in ["sqoop", "merge"]:
        hive_schema = get_conf()['hive_schema']
        dest_adaptor = HiveAdaptor("hive", hive_schema, final_tab_name(), src_adaptor.schemaInfo)

    return dest_adaptor

def get_number_mappers(row_count):
    rows_per_mapper = int(conf.get('rows_per_mapper', 10000000)) * 1.0
    max_mappers = int(conf.get('max_mappers', 50))
    row_count = 0 if row_count == None else row_count
    mappers  = row_count / rows_per_mapper
    mappers = int(mappers) + 1 if mappers > int(mappers) else int(mappers)
    if mappers < 1: 
        mappers = 1 
    elif mappers > max_mappers: 
        mappers = max_mappers
    return mappers

def sqoop_task(dep_task, loc, schemaInfo):
    dsn = conf["dsn"]
    source_adaptor = get_source_adaptor()
    key_str = ",".join(["`%s`"%col for col in source_adaptor.schemaInfo.key_list])
    pk_nm = source_adaptor.get_max_pk_name()
    select_col_str = source_adaptor.get_select_col_str()  
    (schema, tab) = tab_name_tup()
    return {
        "class" : "SqoopTask",
        "dependencies": [ dep_task ],
        "configuration" : {
            "db" : [
                { "adapter": "mysql"},
                { "dsn" : dsn }
            ],
            "command": "import",
            "force" : "true",
            "hadoop_options" :{
                "mapreduce.reduce.java.opts" :"-Xmx8601m",
                "mapreduce.reduce.memory.mb" :12288,
                "mapreduce.map.java.opts" :"-Xmx8601m",   
                "mapreduce.map.memory.mb" :12288, 
                "mapreduce.map.cpu.vcores" :2,    
                "mapreduce.task.timeout" :1000000 },
            "sqoop_params": [
                { "target-dir": loc },
                { "split-by": pk_nm },
                { "fetch-size": -2147483648 },
                { "num-mappers" : get_number_mappers(g_src_max_pkid) },
                { "boundary-query" : "SELECT 0, %s"%(g_src_max_pkid) },
                { "null-string" : "" },
                { "fields-terminated-by" : chr(int(DELIMITER_HEX_STR)) },
                { "null-non-string" : "" },
                { "hive-delims-replacement" : " " },
                { "query": "select %s FROM %s.%s WHERE $CONDITIONS"%(select_col_str, schema, tab ) }
            ]
        }
    }

def hive_task(dep_task, hql):
    return { "class" : "HiveTask",
             "dependencies" : [ dep_task ] ,
             "configuration" : {
                 "hql" : hql }
             }

def check_column_count(dep_task, schemaInfo, teradata_dsn, teradata_schema, tab,create_schema):
    emiter= teradata_schema+"_"+tab+"_column_counts"
    emiter= emiter[0:60]
    emits = [emiter]
    sql_query = "SELECT count(*) as %s FROM DBC.columns WHERE TABLENAME  ='%s' and databasename='%s'" % (emits[0],tab,teradata_schema)
    return { "class" : "SQLRead",
             "dependencies" : [ dep_task ] ,
             "emits": emits ,
             "configuration" : {
                 "db" : {
                     "adapter": "Teradata",
                     "dsn" : teradata_dsn
                 },
                 "sql" : [sql_query]
             }
             }

def check_if_table_exist(dep_task, schemaInfo, teradata_dsn, teradata_schema, tab,create_schema):
    create_table='false'
    emiter=teradata_schema+"_"+tab+"_counts"
    emiter=emiter[0:60]
    if create_schema:
        create_table='true'
    emits = [emiter]
    sql_query = "SELECT count(*) as %s  FROM DBC.TABLES WHERE TABLENAME ='%s' and databasename='%s' and 'true'<>'%s'" % (emits[0],tab,teradata_schema,create_table)
    return { "class" : "SQLRead",
             "dependencies" : [ dep_task ] ,
             "emits": emits ,
             "configuration" : {
                 "db" : {
                     "adapter": "Teradata",
                     "dsn" : teradata_dsn
                 },
                 "sql" : [sql_query]
             }
             }

def load_final_teradata_table(dep_task, schemaInfo, teradata_dsn, teradata_schema, tab,staging_schema,staging_tab):
    delete_statment = "DELETE %s.%s ALL" % ( teradata_schema,tab )
    insert_statment = "INSERT INTO %s.%s SELECT * FROM %s.%s" % (teradata_schema,tab,staging_schema,staging_tab)
    return { "class" : "SQLExecute",
             "dependencies" : [ dep_task ] ,
             "configuration" : {
                 "db" : {
                     "adapter": "Teradata",
                     "dsn" : teradata_dsn
                 },
                 "sql" : [delete_statment,insert_statment]
             }
             }

def delete_teradata_table(dep_task, schemaInfo, teradata_dsn, teradata_schema, tab):
    delete_table_command = "delete  "+teradata_schema+"."+tab+" ALL "
    return { "class" : "SQLExecute",
             "dependencies" : [ dep_task ] ,
             "configuration" : {
                 "db" : {
                     "adapter": "Teradata",
                     "dsn" : teradata_dsn
                 },
                 "sql" : [delete_table_command]
             }
             }

def create_teradata_table(dep_task, schemaInfo, teradata_dsn, teradata_schema, tab,view_schema = None):
    (safe_names, field_list, skip_columns) = get_teradata_field_descs(schemaInfo)
    fields_desc_str = ",\n".join(field_list)
    key_str = ",".join([ safe_names[col] for col in schemaInfo.key_list ])
    #drop before create
    drop_table_command = "call ${dw_udf_lib}.DROP_TABLE_IF_EXISTS ('%s','%s')" % (teradata_schema,tab)
    create_table_query = "CREATE MULTISET TABLE {teradata_schema}.{tab} ,NO FALLBACK , NO BEFORE JOURNAL, NO AFTER JOURNAL, CHECKSUM = DEFAULT, DEFAULT MERGEBLOCKRATIO ( {fields_desc_str} ) PRIMARY INDEX ( {key_str} );".format(**locals())
    emiter1= teradata_schema+"_"+tab+"_column_counts"
    emiter1= emiter1[0:60]
    emiter=teradata_schema+"_"+tab+"_counts"
    emiter=emiter[0:60]
    expression_stmt = "${{{emiter}}} == 0 or ${{{emiter1}}} != ".format(**locals())+`len(schemaInfo.col_list)`
    expression_list=[{"expression":expression_stmt }]
    actions = [drop_table_command,create_table_query]
    if(view_schema != None):
        create_view_statment= "REPLACE VIEW {view_schema}.{tab} AS LOCK ROW FOR ACCESS SEL * FROM {teradata_schema}.{tab}".format(**locals())
        actions.append(create_view_statment)
    return { "class" : "SQLExecute",
             "dependencies" : [ dep_task ] ,
             "conditions":
                 [{ "expression" : expression_stmt }],
             "configuration" : {
                 "db" : {
                     "adapter": "Teradata",
                     "dsn" : teradata_dsn
                 },
                 "sql" : actions
             }
             }

def loadFromHDFS_task(dep_task, schemaInfo, loc, teradata_dsn, teradata_schema, tab, target_name, pattern="part*"):
    final_td_table = "%s.%s" % (teradata_schema, tab)
    load_files = "%s/%s" % (loc, pattern)
    file_size = "file_size_" + target_name
    condition = "( ${%s} > 0 )" % file_size

    (safe_names, field_list, skip_columns) = get_teradata_field_descs(schemaInfo)

    return { "class" : "SQLLoadFromHDFS",
             "dependencies" : [ dep_task ] ,
             "configuration" : {
                 "db" : {
                     "adapter": "Teradata",
                     "dsn" : teradata_dsn
                 },
                 "source_path": load_files,
                 "skip_is_running_check" : "True",
                 "destination_table" : final_td_table,
                 "load_options": {"replace_with_null" : r"\N", "error_limit" : 10000000, "fastload" : "T", "skip_columns" : skip_columns, "parallel" : "T", "tpt_reader.options" : {"TruncateColumnData" : "Y", "TextDelimiterHex" : DELIMITER_HEX_STR} }
             }
             }

def get_named_template(tpl_name):
    tpl_filename = os.path.join(os.path.dirname(__file__), "resources", tpl_name)
    logging.info("Template File: %s" % tpl_filename)
    tpl = Template(filename=tpl_filename)
    return tpl

def save_part_list_to_db(part_list):
    if len(part_list) == 0:
        return

    run_id = get_run_id()
    service_name = get_service_name()
    tab_name = hive_tab()
    process_type = get_run_mode()

    etl_db = EtlStatusDb(conf['etl_status_dsn'])
    etl_db.add_partitions(service_name, tab_name, run_id, process_type, part_list)

def clear_table_metadata():
    if conf['--dryrun']:
        return

    (schema, tab_only) = tab_name_tup()
    etl_db = EtlStatusDb(conf['etl_status_dsn'], get_table_limits_dsn())
    etl_db.clear_table_metadata(get_service_name(), hive_tab(), tab_only)
    
def save_run_status(status, is_merge_or_load = False):
    global g_smart_counters
    
    if conf['--dryrun']:
        return
    run_id = get_run_id()
    service_name = get_service_name()
    mode = get_run_mode()
    tab_name = hive_tab()
    (schema, tab_only) = tab_name_tup()
    table_integrity_state = "SUCCESS"
    
    (source_max_pid, min_id, max_id, increment_count_failure,window_count_failure, max_update) = (0,0,0,0,0, "0000-00-00 00:00:00")
    (min_window_pkid, src_window_count, dest_window_count, src_increment_count, dest_increment_count) = (-1,-1,-1,-1,-1)
    
    etl_db = EtlStatusDb(conf['etl_status_dsn'], get_table_limits_dsn() )
    
    try:
        if status == "SUCCESS":
            mysqlInfo = MysqlInfo(conf['dsn'])
            schemaInfo  = mysqlInfo.get_tab_schema(conf['--tab'])
            hiveInfo = HiveInfo(get_conf()['hive_schema'])
            source_adaptor = get_source_adaptor()
            destination_adaptor = get_destination_adaptor()
            if is_merge_or_load:
                (max_update, min_id, max_id) = hiveInfo.get_latest_update_time(stg_tab_name(), schemaInfo.key_list[0], get_hdfs_temp_loc())
            else:
                (max_update, min_id, max_id) = hiveInfo.get_latest_update_time(final_tab_name(), schemaInfo.key_list[0])

            if mode in ['load']:            
                # Recheck previous count failures
                table_integrity_state = etl_db.recheck_data_integrity_count_failures(source_adaptor, destination_adaptor, get_run_id(), get_service_name(), tab_name, mode)
                # We perform two bounded count checks. One around the increment and the other which is randomized sample within the table, below the increment check
                (min_increment_pkid, max_increment_pkid, min_window_pkid, max_window_pkid) = etl_db.get_integrity_count_ranges(max_id)
                # Perform a Randomized Window Count, only if the increment count didn't cover the entire data set.
                if max_window_pkid > 0:
                    src_window_count = source_adaptor.get_row_count(min_window_pkid, max_window_pkid)
                    dest_window_count = destination_adaptor.get_row_count(min_window_pkid, max_window_pkid)
                    if src_window_count != dest_window_count: 
                        window_count_failure = 1
                src_increment_count = source_adaptor.get_row_count(min_increment_pkid, max_increment_pkid)
                dest_increment_count = destination_adaptor.get_row_count(min_increment_pkid, max_increment_pkid) 

                if src_increment_count != dest_increment_count: 
                    increment_count_failure = 1
            elif mode in ['merge']:
                # Get the counts associated with the previous successful runs. This does not perform count integrity checks for current run
                # So all counts for this run are left at the default: set to -1
                table_integrity_state = g_smart_counters.fetch_and_update_all_counter_results()
        source_max_pid = g_src_max_pkid 
    except Exception as e:
        logging.exception("HARD FAILURE - save_run_status exception: %s" % str(e))
        status = "FAILED"
        
    etl_db.update_etl_status( get_conf()['data_src_platform'], get_cluster_name(), service_name, tab_name, get_target_schema(), tab_only, get_target_table_name(), run_id,  mode, status, get_tab_log_file(), max_update, min_id, max_id, source_max_pid, src_increment_count, dest_increment_count, increment_count_failure, window_count_failure, min_window_pkid, src_window_count, dest_window_count, table_integrity_state, get_min_part())

def is_already_running():
    service_name = conf['service_name']
    mode = get_run_mode()
    tab_name = hive_tab()

    if conf['--dryrun']:
        return False

    etl_db = EtlStatusDb(conf['etl_status_dsn'])
    
    mode = "'sqoop','merge'" if mode in ('sqoop','merge') else "'full_load','load'"

    return etl_db.is_any_have_status(service_name, tab_name,mode,"RUNNING") 

def is_full_load_needed():
    if conf['last_full_load_time'] < conf['last_sqoop_time']:
        return True

    return False

def get_target_info():    
    target_dbs = conf.get('target_database', [])
    targets = []
    for target in target_dbs:
       for name, desc in target.iteritems():
           return (name, desc['dsn'], desc['schema'], desc.get('prefix',''))
           
    return (None, None, None, None)

def get_target_schema():
    if get_run_mode() in ('sqoop','merge'):
        return get_conf()['hive_schema']
    else:
        return get_targets()[0][2]
     
def get_targets():
    target_dbs = conf.get('target_database', [])
    targets = []
    for target in target_dbs:
        for name, desc in target.iteritems():
            desc['name'] = name
            targets.append((name, desc['dsn'], desc['schema'], desc.get('prefix',''),desc['staging_schema'],desc.get('view_schema',None)))
    return targets

def get_last_op_times():
    tab_name = hive_tab()
    service_name = get_service_name()
    etl_db = EtlStatusDb(conf['etl_status_dsn'])
    conf['last_sqoop_time'] = etl_db.get_last_op_time_str(service_name, tab_name,
                                                          'sqoop', 'SUCCESS')
    conf['last_merge_time'] = etl_db.get_last_op_time_str(service_name, tab_name,
                                                          'merge', 'SUCCESS')
    conf['last_load_time'] = etl_db.get_last_op_time_str(service_name, tab_name,
                                                         'load', 'SUCCESS')
    conf['last_full_load_time'] = etl_db.get_last_op_time_str(service_name, tab_name,
                                                              'full_load', 'SUCCESS')
def run_zombie(is_merge_or_load = False):
    """ run zr task """

    if conf['--dryrun']:
        return

    """ TODO: This check should be more transactional, to cover the event that two process attempt to acquire this table at the same time.
    """
   
    """ we have acquired the table, now check for a tmp hive table and remove if it exists
    """
    if is_tmp_hive_table_exists():
        logging.warn("Cleanup from previous run not complete. Deleting tmp hive table: %s"% (get_tmp_hive_table_name() ) )
        if not drop_tmp_hive_table():
            logging.exception("HARD HIVE FAILURE - Could not delete tmp hive table %s" % get_tmp_hive_table_name())
            save_run_status("FAILED")
            return
    try:
        status = "SUCCESS"
        run_zr(get_yml_file(), get_conf())
    except:
        status = "FAILED"

    save_run_status(status, is_merge_or_load)

def create_sqoop_yml(schemaInfo):
    """ create yml file for zr with merge sql """

    make_tab_dir()
    yml_file = get_yml_file()
    dsn = conf["dsn"]

    task_hash = {}
    task_hash["start_task"] = {  "class" : "NopTask"  }
    current_task = "start_task"

    sql = get_sqoop_tab_sql(schemaInfo)

    task_hash["create_hive_tab"] = hive_task(current_task, sql)
    current_task = "create_hive_tab"

    task_hash["sqoop_task"] = sqoop_task(current_task, get_sqoop_tab_loc(), schemaInfo)
    current_task = "sqoop_task"

    sql = get_rename_sqoop_tab_sql()

    task_hash["rename_tab"] = hive_task(current_task, sql)
    current_task = "rename_tab"

    task_hash["end_task"] = {  "class" : "NopTask" ,
                               "dependencies": [ current_task ]  }

    task_hash['settings'] = { "parallelism" : conf['dop'], "wf_cleanup" : 0 }

    logging.info("yml_file: %s" % yml_file)
    with open(yml_file, "w") as fp:
        yml = yaml_util.dump(task_hash )
        print >>fp, yml

def create_full_load_yml(schemaInfo,create_schema):
    """ create yml file for zr full load to td after sqoop """

    make_tab_dir()
    yml_file = get_yml_file()
    dsn = conf["dsn"]

    etl_db = EtlStatusDb(conf['etl_status_dsn'])

    final_tab_loc = final_tab_external_loc()
    logging.info("Full Table being loaded from %s"%final_tab_loc)

    pattern="part*" if final_tab_loc.endswith("_sqoop") else "*"

    task_hash = {}
    task_hash["start_task"] = {  "class" : "NopTask"  }

    tab = target_table_name()

    teradata_dsn = conf.get('teradata_dsn', None)
    teradata_final_schema = conf.get('teradata_final_schema', None)

    targets = get_targets()
    current_task = "start_task"

    for (target_name, target_dsn, target_schema, table_prefix,target_staging_schema,target_view_schema) in targets:
        #finale table name
        td_table_name = get_string_shortener().shorten(table_prefix + tab)
        #staging table name
        full_stg_table_name = "stg_"+td_table_name
        stg_table_name = full_stg_table_name[0:30]
        stg_schema =  target_staging_schema
        view_schema = target_view_schema

        new_task = "check_if_stg_table_exist" + target_name
        task_hash[new_task] = check_if_table_exist(current_task, schemaInfo, target_dsn, stg_schema, stg_table_name,'true')
        current_task = new_task

        new_task = "check_column_count_in_stg_table" + target_name
        task_hash[new_task] = check_column_count(current_task, schemaInfo, target_dsn, stg_schema, stg_table_name,'true')
        current_task = new_task

        new_task = "create_stg_teradata_table" + target_name
        task_hash[new_task] = create_teradata_table(current_task, schemaInfo, target_dsn, stg_schema, stg_table_name)
        current_task = new_task

        new_task = "delete_stg_teradata_tab_" + target_name
        task_hash[new_task] = delete_teradata_table(current_task, schemaInfo, target_dsn, stg_schema, stg_table_name)
        current_task = new_task

        new_task = "load_teradata_stg_tab_" + target_name
        task_hash[new_task] = loadFromHDFS_task(current_task, schemaInfo, final_tab_loc, target_dsn, stg_schema, stg_table_name, target_name, pattern)
        current_task = new_task

        new_task = "check_column_count_in_final_table" + target_name
        task_hash[new_task] = check_column_count(current_task, schemaInfo, target_dsn, target_schema, td_table_name,create_schema)
        current_task = new_task

        new_task = "check_if_td_table_exist" + target_name
        task_hash[new_task] = check_if_table_exist(current_task, schemaInfo, target_dsn, target_schema, td_table_name,create_schema)
        current_task = new_task

        new_task = "create_teradata_tab_" + target_name
        task_hash[new_task] = create_teradata_table(current_task, schemaInfo, target_dsn, target_schema, td_table_name,view_schema)
        current_task = new_task

        new_task = "load_final_teradata_table_" + target_name
        task_hash[new_task] = load_final_teradata_table(current_task, schemaInfo, target_dsn, target_schema, td_table_name,stg_schema,stg_table_name)
        current_task = new_task

    task_hash["end_task"] = {  "class" : "NopTask" ,
                               "dependencies": [ current_task ] }

    task_hash['settings'] = { "parallelism" : conf['dop'], "wf_cleanup" : 0 }

    logging.info("yml_file: %s" % yml_file)
    with open(yml_file, "w") as fp:
        yml = yaml_util.dump(task_hash )
        print >>fp, yml

def get_inner_load_yml(schemaInfo, loc_diff_file):

    inner_tpl = get_named_template('load_inner_yml.tpl')
    (safe_names, field_list, skip_columns) = get_teradata_field_descs(schemaInfo)

    ss = get_string_shortener()
    td_col_str = ",".join([ ss.shorten(col) for col in schemaInfo.col_list])
    ss.reset_used_names()
    td_key_str = ",".join([ ss.shorten(col) for col in schemaInfo.key_list])

    targets = get_targets()
    target_load_yml = ""
    target_names = []
    first_dependent = "merge_task"
    for (target_name, target_dsn, target_schema, tbl_prefix, target_staging_schema, target_view_schema) in targets:
        tabOnly = tbl_prefix + target_table_name()
        teradata_table = get_string_shortener().shorten(tabOnly)
        target_names.append(target_name)
        target_load_yml += inner_tpl.render(teradata_dsn=target_dsn,
                                            first_dependent=first_dependent,
                                            target_name=target_name,
                                            teradata_staging_schema=target_schema,
                                            teradata_final_schema=target_schema,
                                            teradata_table=teradata_table,
                                            skip_columns=skip_columns,
                                            delimiter_hex_str=DELIMITER_HEX_STR,
                                            td_col_str=td_col_str,
                                            td_key_str=td_key_str,
                                            loc_diff_file=loc_diff_file
                                            )
        first_dependent = "apply_diffs_" + target_name

    return (target_load_yml, target_names)

def get_table_run_id():
    return get_service_name() + '/' + get_run_id() + '/' + tab_name()

def get_hdfs_temp_loc():
    return get_megatron_runtime_loc() + '/' + get_table_run_id() + '/'

def create_merge_yml(schemaInfo, add_partition_sql, where_stmt):
    global g_smart_counters
    """ create yml file for zr with merge  and load sql """

    make_tab_dir()

    run_mode = get_run_mode()

    # Template based yaml generation
    tab = tab_name()

    conf_table_info = conf.get('table_info', {}).get(tab, {})
    num_reducers = conf_table_info.get('num_reducers', None)
    hive_set = ""
    if  num_reducers:
        hive_set = "set mapred.reduce.tasks=%d;"%num_reducers

    quoted_list = []
    for col in schemaInfo.col_list:
        if col in HIVE_RESERVED:
            quoted_list.append("`%s`"%col)
        else:
            quoted_list.append("%s"%col)
    col_str = ",".join(quoted_list)

    data_filter_str = ",".join(schemaInfo.data_filter_list())
    key_str = ",".join(["%s"%col for col in schemaInfo.key_list])
    col_arg_str = ",".join(["%s"%col for col in schemaInfo.col_list])
    col_def_str = ",".join([ "`%s` string"%col for col in schemaInfo.col_list ])

    service_name=get_service_name()
    etl_db = EtlStatusDb(conf['etl_status_dsn'])
    stg_tab = stg_tab_name()

    yml_file = get_yml_file()
    logging.info("Creating yml %s"%yml_file)
    
    with open(yml_file, "w") as f_script:
        g_smart_counters = SmartCounters()
        ds = strftime("%Y-%m-%d", gmtime())
        ts = strftime("%H%M%S", gmtime())
        (schema, tabOnly) = tab_name_tup()
        loc = conf['final_table_loc']
        loc_diff_tab =  "{loc}/{service_name}/{schema}_{tabOnly}".format(**locals())
        loc_diff_file = "{loc_diff_tab}/ds={ds}/ts={ts}".format(**locals())
        tpl = None
        load_inner_yml = ""
        if run_mode == "load":
            (load_inner_yml, target_names) = get_inner_load_yml(schemaInfo, loc_diff_file)
            tpl = get_named_template('load_yml.tpl')
        else:
            tpl = get_named_template('merge_yml.tpl')
            target_names = []

        workflow_id = get_run_id()
        hive_schema = conf['hive_schema']
        yml = tpl.render(load_inner_yml=load_inner_yml,
                         etl_status_dsn=conf['etl_status_dsn'],
                         host_name=EtlStatusDb.host_name,
                         service_name=service_name,
                         hive_schema=hive_schema,
                         target_names=target_names,
                         schema=schema,
                         tab=tabOnly,
                         ds=ds,
                         ts=ts,
                         delimiter_hive=DELIMITER_HIVE,
                         delimiter_hex_str=DELIMITER_HEX_STR,
                         stg_tab=stg_tab,
                         col_def_str=col_def_str,
                         loc=loc,
                         hdfs_cur_extern_loc=final_tab_external_loc(),
                         purge_list=etl_db.get_purge_list(service_name, MAX_TRASH_AGE),
                         loc_diff_file=loc_diff_file,
                         loc_diff_tab=loc_diff_tab,
                         parallelism=conf['dop'],
                         workflow_id=workflow_id,
                         add_partition_sql=add_partition_sql,
                         hive_set=hive_set,
                         script_dir=g_script_dir,
                         where_stmt=where_stmt,
                         col_arg_str=col_arg_str,
                         col_str=col_str,
                         key_str=key_str,
                         data_filters=data_filter_str,
                         hdfs_temp_loc=get_hdfs_temp_loc(),
                         count_tasks=g_smart_counters.get_tasks_descriptor()
                         )
        logging.info("%s Yaml (from template): %s" %(run_mode, yml_file))
        print>>f_script, yml
        return

def full_load_main():
    """ Do full load after sqoop """
    # check if sqoop is needed
    full_load_needed = is_full_load_needed()

    if not full_load_needed:
        logging.info("full load not needed")
        if get_conf()["--force"]:
            logging.info("Force option enabled. Continue anyway.")
        else:
            save_run_status("NOT_NEEDED")
            return

    yml_file = get_yml_file()
    logging.info("yml_file: %s" % yml_file)

    mysqlInfo = MysqlInfo(conf['dsn'])
    tab = tab_name()

    # get table schema
    schemaInfo  = mysqlInfo.get_tab_schema(tab)

    create_full_load_yml(schemaInfo,conf["--create_schema"])

    # run zr with yml file
    run_zombie()

def sqoop_main():
    
    tab = tab_name()
    mysqlInfo = MysqlInfo(conf['dsn'])

    # get table schema
    schemaInfo  = mysqlInfo.get_tab_schema(tab)

    # check if sqoop is needed
    sqoop_needed = is_sqoop_needed(schemaInfo)

    if not sqoop_needed:
        if get_conf()["--force"]:
            logging.info("Sqoop not needed. Continue ... in force mode")
        else:
            save_run_status("NOT_NEEDED")
            logging.info("Sqoop not needed")
            return

    yml_file = get_yml_file()
    logging.info("yml_file: %s" % yml_file)
    create_sqoop_yml(schemaInfo)

    # run zr with yml file
    run_zombie()

def merge_main():

    yml_file = get_yml_file()

    run_mode = get_run_mode()

    stg_dirs = get_stg_tab_dirs()
    if not stg_dirs and not get_conf()["--force"]:
        if get_conf()["--force"]:
            logging.info("No staging dirs exists, but FORCE continue")
        else:
            logging.info("No staging dirs exists")
            save_run_status("NOT_NEEDED")
            return

    stg_dirs = []

    mysqlInfo = MysqlInfo(conf['dsn'])
    tab = tab_name()
    hive_tab = tab_name()

    # get table schema
    schemaInfo  = mysqlInfo.get_tab_schema(tab)

    if not schemaInfo.key_list:
        logging.info("No primary keys for this table, merge can not be done")
        save_run_status("NOT_NEEDED")
        return

    if run_mode == "merge":
        # check if sqoop is needed
        sqoop_needed = is_sqoop_needed(schemaInfo)
        if  sqoop_needed and not conf['--dryrun']:
            logging.info("Sqoop needed in merge mode")
            save_run_status("NOT_NEEDED")
            return
    elif run_mode == "load":
        sqoop_needed = is_sqoop_needed(schemaInfo)
        if  sqoop_needed and not conf['--dryrun']:
            logging.info("Sqoop needed in load mode")
            save_run_status("NOT_NEEDED")
            return
        full_load_needed = is_full_load_needed()
        if  full_load_needed:
            logging.info("full load needed in load mode")
            save_run_status("NOT_NEEDED")
            return

    # create external stg table on tungsten data
    create_stg_tab(schemaInfo)

    # generate add partition sql for stg table 
    (where_stmt, add_part_stmt, part_list) = gen_add_partition_sql()

    if not add_part_stmt:
        if get_conf()["--force"]:
            logging.info("Merge being FORCED with No partitions ")
        else:
            logging.info("No parts to merge")
            save_run_status("NOT_NEEDED")
            return

    # create yml file
    create_merge_yml(schemaInfo, add_part_stmt, where_stmt)

    # 
    save_part_list_to_db(part_list)

    # run zr with yml file
    run_zombie((where_stmt is not None))


"""
    Sqoop:
        -- when schema changes
    full_load:
        -- sqoop_time > last_full_load : sqoop happened after last full load, then re-load
    merge:
       -- no changes to schema & partitions exists
    load:
      -- sqoop_time < last_full_load &  partitions exists

"""
def main():
    try:

        if is_already_running():
            save_run_status("ALREADY_RUNNING")
            logging.warn("Another %s process already running"%get_run_mode())
            return

        save_run_status("RUNNING")

        if get_run_mode() == "sqoop":
            sqoop_main()
        elif get_run_mode() == "full_load":
            full_load_main()
        elif get_run_mode() == "merge":
            merge_main()
        elif get_run_mode() == "load":
            merge_main()
    except Exception as e:
        logging.exception("HARD FAILURE::main() exception trapped: %s" % str(e))
        save_run_status("FAILED")

if __name__ == '__main__':
    args = docopt(__doc__)

    level_map = {0: logging.WARNING, 1: logging.INFO, 2: logging.DEBUG}
    format = '%(asctime)s %(levelname)s [%(name)s:%(lineno)d] - %(message)s'
    logging.basicConfig(level=level_map[args['-v']], format=format)

    conf = load_conf(args)

    if conf['--dryrun']:
        logging.info("Running in dryrun mode....")

    if conf['--test_mode']:
        logging.info("Running test....")

    get_last_op_times()
    if get_run_mode() not in ("sqoop", "full_load", "merge", "load"):
        raise Exception("Unnkown run_mode %s"%conf["--run_mode"])
    main()

