
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
from shared import os_util
from shared  import hdfs, util
from shared  import hive_util
from shared  import yaml_util
from mysqlInfo import MysqlInfo
from config import *
from zombie_runner.shared.string_utils import *

MAX_CHAR_LEN = 32000
TEXT_CHAR_LEN = 512
TEXT_TYPES = ["text", "tinytext", "mediumtext", "longtext"]

def get_string_shortener():
    return StringShortener(mode = get_conf().get('string_shortener','hash'), max_len = 30)    

def get_def_value(data_def, col_name):
    val = data_def[col_name]
    return "" if val == None else val

def convert_to_td_int(data_def, override):
    data_type = data_def['DATA_TYPE']
    data_len = int(data_def['NUMERIC_PRECISION'])

    if data_type in ["tinyint", "smallint"]:
        td_data_type = "smallint"
    elif data_type in ["bigint"]:
        td_data_type = "bigint"
    else:
        td_data_type = "integer"

    if "unsigned" in get_def_value(data_def, 'COLUMN_TYPE'):
        data_len += 1
    return td_data_type

def convert_to_td_varchar(data_def, override):
    data_type = data_def['DATA_TYPE']
    data_len = int(data_def['CHARACTER_MAXIMUM_LENGTH'])

    if data_type == "varbinary":
        data_len = 2 * data_len
    elif data_type in TEXT_TYPES:
        data_len = 1
    
    if override:
        data_len = override.get("length", 256)
        
    data_len = MAX_CHAR_LEN if data_len > MAX_CHAR_LEN else data_len

    unicode_str = ""
    if "utf8" in get_def_value(data_def, 'CHARACTER_SET_NAME'):
        char_set = "UNICODE"
    else:
        char_set = "LATIN"

    return "varchar({data_len}) CHARACTER SET {char_set} CASESPECIFIC".format(**locals())

def convert_to_td_char(data_def, override):
    data_type = data_def['DATA_TYPE']
    data_len = int(data_def['CHARACTER_MAXIMUM_LENGTH'])

    if data_type == "binary":
        data_len = (2 * data_len) + 4

    unicode_str = ""
    if "utf8" in get_def_value(data_def, 'CHARACTER_SET_NAME'):
        char_set = "UNICODE"
    else:
        char_set = "LATIN"

    return "char({data_len}) CHARACTER SET {char_set} CASESPECIFIC".format(**locals())

def convert_to_td_decimal(data_def, override):
    data_type = data_def['DATA_TYPE']
    precision = int(data_def['NUMERIC_PRECISION'])
    scale = int(data_def['NUMERIC_SCALE'])
    return "decimal({precision}, {scale})".format(**locals())


def get_teradata_field_descs(schemaInfo):
    col_hash = schemaInfo.col_hash
    column_descriptors = []
    safe_names = dict()
    skip_columns = []

    u_id = 0
    ss = get_string_shortener()
    for col in schemaInfo.col_list:
        data_def = schemaInfo.col_hash[col]
        col_type = data_def["DATA_TYPE"]
        safe_name = ss.shorten(col)
        override = get_column_override(col)
        if col_type in ["varchar", "varbinary"]:
            converter = convert_to_td_varchar
        elif col_type in TEXT_TYPES:
            converter = convert_to_td_varchar
            if not override:
                skip_columns.append(safe_name)
        elif col_type in ["char", "binary", "enum"]:
            converter = convert_to_td_char
        elif col_type in ["tinyint", "smallint", "mediumint", "int", "bigint"]:
            converter = convert_to_td_int
        elif col_type in ["decimal"]:
            converter = convert_to_td_decimal
        elif col_type in ["float"]:
            converter = lambda x, override: "float"
        elif col_type in ["datetime", "timestamp"]:
            converter = lambda x, override: "timestamp(6)"
        elif col_type in ["date", "time"]:
            converter = lambda x, override: col_type
        else:
            converter = lambda x, override: "varchar(666) CHARACTER SET LATIN CASESPECIFIC "

        safe_names[col] = safe_name
        column_descriptors.append("%s %s" % (safe_names[col], converter(data_def, override)))

    return (safe_names, column_descriptors, skip_columns)

