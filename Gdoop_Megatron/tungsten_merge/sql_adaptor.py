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
import shlex

from shared import os_util
from shared  import hdfs
from shared  import hive_util
from shared  import hdfs
from shared  import yaml_util
from shared.sql  import *

BOUNDED_COUNT_RANGE = 100000

class SQLAdaptor(object):
        
    binary_types = ["VARBINARY", "BINARY"]
    datetime_types = ["DATETIME", "TIMESTAMP"]

    def __init__(self, dsn, schema, table_name, schemaInfo=None):
        self.dsn = dsn
        self.sql_adaptor = None
        self.cfg = None
        self.adaptor_name = None
        self.table_name = table_name
        self.schema = schema
        self.zrc2_config = None
        zrc_file = os.environ.get('ZOMBIERC')
        with open(zrc_file, "r") as fp:
            self.zrc2_config = yaml_util.load(fp)
        
        if dsn == 'hive':
            self.adaptor_name = "Hive"
        else:
            cfg = ODBCConfig(dsn).get_cfg()
            self.driver = cfg.get("driver", "")
            if "libmyodbc" in self.driver:
                self.adaptor_name = "MySQLDirect"
            elif "tdata.so" in self.driver:
                self.adaptor_name = "Teradata"
            self.sql_adaptor = SQL(dsn=dsn, adapter=self.adaptor_name)
        
        self.schemaInfo = self.sql_adaptor.get_table_schema(self.table_name) if schemaInfo==None else schemaInfo
        
    def data_filter_list(self):
        data_filters = []
        for col in self.schemaInfo.col_list:
            cfilter = "none"
            ctype = self.schemaInfo.col_hash[col]["DATA_TYPE"].upper()
            if ctype in SQLAdaptor.binary_types:
                olen = self.schemaInfo.col_hash[col]["CHARACTER_OCTET_LENGTH"]
                if olen == 16:
                    cfilter = "uuid"
            elif ctype in SQLAdaptor.datetime_types:
                cfilter = "datetime"
            elif ctype == "DATE":
                cfilter = "date"
            data_filters.append(cfilter)
        return data_filters

    def get_bounding_sql(self, min_id = None, max_id = None):
        pkid = self.schemaInfo.key_list[0]
        
        min_id = 0 if min_id == None else int(min_id)
        max_id = int(max_id)
        
        if (max_id - min_id) > BOUNDED_COUNT_RANGE:
            new_min_id = max_id - BOUNDED_COUNT_RANGE
            logging.warn("get_bounding_sql bounding range exceeds maximum %d. minimum pkid changed from: %d to: %d"%(BOUNDED_COUNT_RANGE, min_id, new_min_id))
            min_id = new_min_id
                        
        return "select count(*) from %s.%s WHERE %s >= %d AND %s <= %d"%(self.schema, self.table_name, pkid, min_id, pkid, max_id)

    def get_max_pkid_sql(self):
        pkid = self.schemaInfo.key_list[0]
        return "select max(%s) from %s.%s"%(pkid, self.schema, self.table_name)
    
    def get_max_pk_name(self):
        return self.schemaInfo.key_list[0]
        
    def get_row_count(self, min_id = None, max_id = None):
        if min_id != None: min_id = int(min_id)
        if max_id != None: max_id = int(max_id)

        if max_id == 0:
            return 0
            
        pkid = self.schemaInfo.key_list[0]
        where = ""
        if min_id != None:
            where = "WHERE %s >= %s" % (pkid, min_id)
        if max_id != None:
            if len(where) > 1:
                where = where + " AND %s <= %s" % (pkid, max_id)
            else:
                where = "WHERE %s <= %s" % (pkid, max_id)
        
        sql = self.get_bounding_sql(min_id, max_id)
        logging.info("get_row_count: %s"%(sql))
        try:
            with self.sql_adaptor.get_cursor() as cur:
                cur.execute(sql)
                row = cur.fetchone()
                row_cnt = int(row[0])
        except:
           row_cnt = 0

        return row_cnt

    def get_max_pkid(self):
        sql = self.get_max_pkid_sql()
        try:
            with self.sql_adaptor.get_cursor() as cur:
                cur.execute(sql)
                row = cur.fetchone()
                pkid = row[0]
        except:
           pkid = 0 

        try:
            pkid = int(pkid) 
        except:
            pkid = 0

        return pkid

    def get_select_col_str(self):
        cast_types = ["DATE", "DATETIME", "TIMESTAMP", "TIME", "BIGINT", "TINYINT"]
        binary_types = ["VARBINARY", "BINARY"]
        select_list = []
        for col in self.schemaInfo.col_list:
            type = self.schemaInfo.col_hash[col]["DATA_TYPE"].upper()
            if type in cast_types:
                select_list.append("cast(`%s` as char)"%col)
            elif type in binary_types:
                olen = self.schemaInfo.col_hash[col]["CHARACTER_OCTET_LENGTH"]
                if olen == 16:
                    select_list.append( "LOWER(CONCAT(LEFT(HEX(`{col}`), 8), '-', MID(HEX(`{col}`), 9,4), '-', MID(HEX(`{col}`), 13,4), '-', MID(HEX(`{col}`), 17,4), '-', RIGHT(HEX(`{col}`), 12)))".format(**locals()) )
                else:
                    select_list.append("LOWER(HEX(`%s`))"%col)
            else:
                select_list.append("`%s`"%col)

        select_col_str = ",".join(select_list)   
        return select_col_str

class HiveAdaptor(SQLAdaptor):

    def __init__(self, *args, **kwargs):
        super(HiveAdaptor, self).__init__(*args, **kwargs)
        zr_settings = self.zrc2_config.get('settings', {})
        self.fair_scheduling_queue = zr_settings.get('fair_scheduling_queue', 'default')

    def get_max_pkid_sql(self):
        pkid = self.schemaInfo.key_list[0]
        return "select max(cast(%s, int)) from %s.%s"%(pkid, self.schema, self.table_name)
    
    def execute_hql(self, sql):
        cmd = [hive_util.get_hive(), "-e",  "set mapred.job.queue.name=%s; %s"% (self.fair_scheduling_queue, sql)]
        (result, stdout, stderr) = os_util.execute_command(cmd, do_sideput=True)
        if result != 0:
            logging.info("failed run  cmd="+`cmd`+" stdout="+stdout+" stderr="+stderr)
            return 0
        return (result, stdout, stderr)

    def execute_hql_get_int(self, sql):
        int_value = 0
        cmd = [hive_util.get_hive(), "-e",  "set mapred.job.queue.name=%s; %s"% (self.fair_scheduling_queue, sql)]
        (result, stdout, stderr) = os_util.execute_command(cmd, do_sideput=True)
        if result != 0:
            logging.info("failed run  cmd="+`cmd`+" stdout="+stdout+" stderr="+stderr)
        elif result == 0:
            try:
                int_value = int(stdout.strip())
            except:
                pass
            
        return int_value

    def get_row_count(self,  min_id = None, max_id = None):
        if min_id != None: min_id = int(min_id)
        if max_id != None: max_id = int(max_id)        
        if max_id == 0 or min_id == -1:
            logging.info("get_row_count max_id = %d, min_id = %d. Returning 0" % (max_id, min_id))
            return 0
            
        sql = self.get_bounding_sql(min_id, max_id)
        logging.info("Executing: %s" % (sql))
        return self.execute_hql_get_int(sql) 

    def get_max_pkid(self):
        """ get_row_count """
        sql = self.get_max_pkid_sql()
        return self.execute_hql_get_int(sql) 
