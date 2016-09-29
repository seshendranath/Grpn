#!/usr/bin/env python

"""
Usage:
  create_hive_table.py [--backup_dir=<backup_dir>] [--done_dir=<done_dir>] [--table_name=<schema.table>] [--part=<yyyymmdd>] [-v|-vv] [--hive_table=<hschema.htable>]
  create_hive_table.py  (-h|--help)

Options:
    --table_name=<schema.table> table name used in hdfs folder
    --part=<yyyymmdd> 	specific partition to add , relevant only for partitioned tables if not specified, all HDFS folders are being checked and added in Hive
    --backup_dir=<backup_dir> dir in which hdfs  data read from, if not specified uses default /user/grp_gdoop_edw_rep_prod
    --done_dir=<done_dir> dir to dir where backup done files are created, if not specified uses /user/grp_gdoop_edw_rep_prod/done
    --hive_table=<hschema.htable> hive table name specified here overrides default table_name parameter used for recreating partitioned tables in case DDL changed or troubleshooting 
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

logging.getLogger().setLevel(logging.DEBUG)
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
logging.getLogger().addHandler(ch)

g_backup_dir = "hdfs://gdoop-namenode-vip.snc1/user/grp_gdoop_edw_rep_prod"
g_done_dir = g_backup_dir + "_done"
BACKUP_SCHEMA = "/user/grp_gdoop_edw_rep_prod";
hive_keywords=['comment','bucket','location']

def is_exe(fpath):
    return os.path.exists(fpath) and os.access(fpath, os.X_OK)
def get_hadoop():
    return which("hadoop")
def get_hive():
    return which("hive")

def which(program): # http://stackoverflow.com/a/377028/625497
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
    cmd = [get_hadoop(), "fs", '-mkdir', '-p',  path]
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
    iterations = 1
    
    #for i in range(0,iterations):
    logging.info("Running "+" ".join(cmd))
    (result, stdout, stderr) = execute_command(cmd)
	  #if result != 0:
	  #   time.sleep(15)
	  #else:
    if result != 0:
        print '...Sending email unable to execute HIVE'
        send_email('HIVE ERROR', 'unable to execute HQL repeatedly ' + query)
        raise Exception("Process ended abnormally: [%s]" % stderr)
    else:
        return result
    
def table_schema(schema , table_name):
    """ executes hive describe <table_name>'. Returns results"""
    cmd = [get_hive(), "-S", "-e",  '"use %s;describe formatted %s"'%(schema, table_name)]
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
    start = True
    for line in stdout.split('\n'):
        line = line.strip()
        if not line or (line[0] != "#" and start):
            continue
        start = False
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
    return [ line.strip() for line in stdout.split('\n') if line.strip() and ("WARN") not in line.strip() ]

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
        if line.strip() == "(":   #find the opening bracket for column list collection
            start_hash = True
            continue

        if not start_hash:	  #skip lines until start_hash is True
            continue

        if len(line.strip()) > 0 and line.strip()[0]=="(":   #not an empty line, because long columns can contain such a line, and ( ????
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

        if line[0:3] == '   ':		#rely on manifest file output starts with 3 spaces, is that a safe assumption? 
            if arr[0].strip('"') in hive_keywords:
	         arr[0]='`'+arr[0].strip('"')+'`'
	    if arr[0].strip().startswith('_'):
	         arr[0]=arr[0].lstrip('_')+'_'
	    col_list.append((arr[0].lower(), get_hive_type(arr[1])))

        if line[-1] == ")":		#end of column def collection	
            break
    return col_list

def send_email(subject,message):
        msg = MIMEText(message) # Create a text/plain message

	sender = 'edw-infra@groupon.com'
        #recipients = ['edw-infra@groupon.com', 'edw-dev-ops@groupon.com', 'aguyyala@groupon.com', 'kbohra@groupon.com']
        recipients = ['aguyyala@groupon.com', 'kbohra@groupon.com']

        msg['Subject'] = subject
        msg['From'] = sender
        msg['To']   = ", ".join(recipients)

        s = smtplib.SMTP('localhost')
        s.sendmail(sender,recipients,msg.as_string())
        s.quit()

def is_new_ddl(schema_name, table_name, col_list,schema_hash): #col_list is from manifest
    							       #schema_hash from hive
    if 'cols' in schema_hash:
        col_list_hive = schema_hash['cols']
        if not col_list_hive:
            return 
        if 'col_name' in col_list_hive:
           del col_list_hive['col_name']; # remove the col_name header that should not be part of the hash
    
    col_list_manifest = {key.strip('`'): item.strip() for key, item in dict(col_list).items()} #remove ` added on hive keywords
    if cmp(col_list_hive,col_list_manifest) == 0:
        return 
    else:
        if (cmp(col_list_hive,col_list_manifest) == -1 ):
		print "Missing from hive" + str(col_list_hive)
		return set(col_list_manifest) - set(col_list_hive)
	else:
		print "Missing from manifest" + str(col_list_manifest)
		return  set(col_list_hive) - set(col_list_manifest)

def create_full_tab(schema_name, table_name, col_list, data_file,schema_hash):
    """ create schema for dimension table pointing to the latest backup"""
    table_name = table_name.translate(None, '!@#$')

    cols = ",".join([ "%s %s"%item for item in col_list ])
    loc = os.path.dirname(data_file)

    sql = """
          USE {schema_name};
          DROP TABLE if exists {table_name};
          CREATE EXTERNAL TABLE IF NOT EXISTS {table_name}
          ({cols})
          ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\001' ESCAPED BY '\\\\'
              stored as
                      inputformat  'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
                      outputformat 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
          location '{loc}'
	  """.format(**locals())
    hive_query(sql)
    #TBLPROPERTIES('serialization.null.format'='')

def create_part_tab(schema_name, table_name, col_list,schema_hash):
    """ create table for fact tables, if it does not exists """
    table_name = table_name.translate(None, '!@#$')

    cols = ",".join([ "%s %s"%item for item in col_list ])
    sql = """
            USE {schema_name}; CREATE EXTERNAL TABLE IF NOT EXISTS  {table_name}
            ({cols}) partitioned by (ds string)
            location '/tmp'""".format(**locals())
    hive_query(sql)

def alter_part_tab(schema_name, table_name, schema_hash, manifest_col_list):
    """ add new columns to the fact table if the schema has changed"""

    if (is_new_ddl(schema_name, table_name, manifest_col_list,schema_hash)):
	send_email('HIVE DDL mismatch '+ schema_name+'.'+ table_name,
		'Manifest columns\n'+str(manifest_col_list) +'\nHive columns\n' + str(schema_hash)+ '\nDiff\n' + str(result));
    return
 
    #block comment start due to return above
    org_cols = schema_hash['cols'] #hive list
    new_col_list = []
    
    for item in manifest_col_list:
         if org_cols.has_key(item[0]):
            continue
         new_col_list.append((item[0],item[1]))
    
    if not new_col_list:
        return
    
    cols = ",".join([ "%s %s"%item for item in new_col_list])
    sql = """
            USE {schema_name};ALTER TABLE {table_name} add columns({cols})
        """.format(**locals())
    hive_query(sql) #block code commented due to return above

def add_partition(schema_name, table_name, part_list):        
    """ Add new partitions for fact tables """
    full_name = schema_name+"."+table_name
    hive_parts = table_partitions(full_name)	
    #logging.info("HIVE show partitions "+ str(hive_parts))
    #logging.info("HDFS partitions "+ str(part_list))
    hive_parts.sort()
    
    last_tab_part = None
    tab_part_set = set([])
    for tp in hive_parts:
        tab_part_set.add(tp.split('=')[1]) #set of all hive partitions 
    if hive_parts:
        last_tab_part = hive_parts[-1].split('=')[1]
   
    add_part_list = [] 		# structure for partitions to add in HIVE
    
    for part in part_list:	#iterate over hdfs parts 
        ds = os.path.basename(part)            
        #if ds in tab_part_set:	# if partition already exists in table
        #    continue => we still want to reset partition elements in case of backfilling
        sql = """
		ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION (ds='{ds}') ;
		ALTER TABLE {table_name} PARTITION (ds='{ds}') SET LOCATION '{part}/lzo';
		ALTER TABLE {table_name} PARTITION (ds='{ds}') SET SERDEPROPERTIES ('field.delim' = '001','escape.delim' = '\\\\');
                ALTER TABLE {table_name} PARTITION (ds='{ds}') SET FILEFORMAT INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe';

		""".format(**locals())
 
 	add_part_list.append(sql);

    if not add_part_list:
        return

    for i in range(1,100):
        range_list =add_part_list[(i-1)*100:i*100]
        if not range_list:
            break;
        add_part_str = ";".join(range_list)
        sql = "use {schema_name}; {add_part_str}".format(**locals())
        #print sql
        hive_query(sql)

#this function below is not in use
def rename_current_tab(schema_name, table_name, schema_hash,col_list):
    """
        move the current dimension table td_backup_archive schema
    """

    loc = schema_hash['details']['Location:']
    cols = ",".join([ "%s %s"%item for item in col_list ])
    date_id = os.path.basename(os.path.dirname(loc))
    archive_schema = 'td_backup_archive'
    sql = """
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
    hive_table = get_args(args, "--hive_table", "")
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
            part_list = [ tab+"/"+part ] 			#['hdfs://gdoop-namenode-vip.snc1/td_backup/prod_groupondw.agg_mob_imp_funnel/20141221']
        else:
            part_list = hdfs_ls(tab)				#get all days from hdfs
        logging.info("Num parts %d", len(part_list))
        if len(part_list) == 0:
            logging.error("No partitions for "+tab);
            continue
        part_list.sort()

        latest_part = part_list[-1]

        logging.info("latest part "+latest_part)

        schema_file = get_manifest(latest_part)
        if not schema_file:
             logging.info("No schema file present for "+tab);
             continue

	manifest_col_list = get_column_list(schema_file)
        #logging.info("manifest col list => "+str(manifest_col_list) )

	full_bkp = is_full_bkp(schema_file)
        logging.info("full_bkp="+str(full_bkp))

        data_file = get_data(latest_part)
        if not data_file and full_bkp:
            logging.info("No data file present for "+tab);
            continue 				# we should not skip partitions for incr backups, it is however 
						# safer to not proceed if there is no data for full snapshots
	
	sch_list=[BACKUP_SCHEMA, schema_name]	# default list
	if hive_table:				# if hive table param overrides the default schema in hive
		(hive_s,hive_t) = hive_table.split('.')
		table_name = hive_t
		sch_list=[hive_s] 		# overwrite hive table list if hive_table argument passed

	for lschema in sch_list:
	  schema_hash = table_schema(lschema, table_name) 	# hive describe formatted 
	  logging.info("Schema hash "+`schema_hash`)
          if full_bkp:	 # handle dimension table here
            #if schema_hash['details']:
		#we dont want to keep full table DDL in archive database anymore, no real use case
		#rename_current_tab(BACKUP_SCHEMA, table_name, schema_hash, manifest_col_list)
            create_full_tab(lschema, table_name, manifest_col_list, data_file, schema_hash)

          else:		 # handle fact tables here
	    if not schema_hash['details']: 
		# new Hive table because details are missing in hive
		create_part_tab(lschema, table_name, manifest_col_list,schema_hash)
            else:
                alter_part_tab(lschema, table_name, schema_hash, manifest_col_list)
            add_partition(lschema, table_name, part_list)
	  #end of for loop 

        done_file = schema_file.replace(g_backup_dir, g_done_dir)+".done"
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
