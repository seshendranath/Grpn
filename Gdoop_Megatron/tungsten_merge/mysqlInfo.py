#!/usr/bin/env python

"""
Usage:
  main.py  --tungsten_conf=<conf_file> --db_conf=<conf_file>  [--dryrun]   [-v|-vv] 
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


TabInfo = namedtuple('TabInfo', ['schema', 'tab', 'context', 'create_time'])

#SchemaInfo = namedtuple('SchemaInfo' , [ 'key_list', 'col_list', 'col_hash' ])

class SchemaInfo:
    binary_types = ["VARBINARY", "BINARY"]
    datetime_types = ["DATETIME", "TIMESTAMP"]
    
    def __init__(self, key_list, col_list, col_hash):
        self.key_list = key_list
        self.col_list = col_list
        self.col_hash = col_hash
    
    def data_filter_list(self):
        data_filters = []
        for col in self.col_list:
            cfilter = "none"
            ctype = self.col_hash[col]["DATA_TYPE"].upper()
            if ctype in SchemaInfo.binary_types:
                olen = self.col_hash[col]["CHARACTER_OCTET_LENGTH"]
                if olen == 16:
                    cfilter = "uuid"
            elif ctype in SchemaInfo.datetime_types:
                cfilter = "datetime"
            elif ctype == "DATE":
                cfilter = "date"
            data_filters.append(cfilter)
        return data_filters
    
class MysqlInfo:
    def __init__(self, dsn):
        self.dsn = dsn
        self.mysqlDirect = SQL(dsn=dsn, adapter="MySQLDirect")
        self.cfg = ODBCConfig(dsn)
        
    def is_add_table(self,full_tab_name, tab, include_tab_list, exclude_tab_list):
  
        # check if table is excluded
        include_tab = False
        tab_context = {}
        
        if full_tab_name in exclude_tab_list:
            return (False, tab_context)

        # check if there is a include list
        if not include_tab_list:
            include_tab = True
        else:
            for elem in include_tab_list:
                key = elem if type(elem) is not dict else elem.keys()[0]
                if full_tab_name == key:
                    include_tab = True
                    tab_context = elem.values()[0] if type(elem) is dict else {}
        
        if 'table_name' not in tab_context:
            tab_context['table_name'] = tab
                   
        return (include_tab, tab_context)
                    
    def get_table_list(self, include_tab_list, exclude_tab_list):
        
        tab_list = []
        sql = """
            select  TABLE_SCHEMA, TABLE_NAME, CREATE_TIME 
            from information_schema.TABLES 
            where TABLE_SCHEMA=database() and TABLE_TYPE='BASE TABLE'
            """
        with self.mysqlDirect.get_cursor() as cur:
           cur.execute(sql) 
           rows = cur.fetchall()
           for row in rows:
             (schema, tab, create_time) = row
             full_tab_name = schema+"."+tab
             (include_tab, tab_context) = self.is_add_table(full_tab_name, tab, include_tab_list, exclude_tab_list)
             if include_tab:
                 tabInfo = TabInfo(schema, tab, tab_context, create_time)
                 tab_list.append(tabInfo)
        return tab_list

    def get_max_pkid(self, schemaInfo, schema_name, tab):
        pkid = schemaInfo.key_list[0]
        sql = "select max(%s) from %s.%s"%(pkid, schema_name, tab)
        with self.mysqlDirect.get_cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
        return row[0]
            
    def get_tab_schema(self, tab):
        (schema, tab) = tab.split('.')

        sql = """SELECT
                    COLUMN_NAME, DATA_TYPE, COLUMN_KEY, ORDINAL_POSITION, CHARACTER_MAXIMUM_LENGTH, CHARACTER_OCTET_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, CHARACTER_SET_NAME, COLUMN_TYPE
                FROM information_schema.columns
                WHERE TABLE_NAME='%s' AND TABLE_SCHEMA=database() order by ORDINAL_POSITION
                """%( tab )

        schema_file = os.getenv("_TEST_SHCEMA_FILE", None)
        if not schema_file:
            with self.mysqlDirect.get_cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall()
        else:
            import json
            with open(schema_file) as fh:
                rows = json.load(fh)

        key_list = []
        col_list = []

        col_hash = {}
        for row in rows:
            if row[2] == "PRI":
                key_list.append(row[0])
            col_list.append(row[0])
            col_hash[row[0]] = {"DATA_TYPE" : row[1] ,"CHARACTER_MAXIMUM_LENGTH" : row[4], "CHARACTER_OCTET_LENGTH" : row[5], "NUMERIC_PRECISION" : row[6], "NUMERIC_SCALE" : row[7], "CHARACTER_SET_NAME" : row[8], "COLUMN_TYPE" : row[9]} 
        schemaInfo = SchemaInfo(key_list, col_list, col_hash)
        
        return schemaInfo


