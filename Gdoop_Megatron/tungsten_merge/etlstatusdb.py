import MySQLdb
import sys
import random
from zombie_runner.shared.sql import SQL
import logging 
from shared import os_util
import util
import time
import config
from sql_adaptor import *
from datetime import datetime
from datetime import timedelta

EVENTMAP = {
            "merge": ("hive", "loaded"),
            "sqoop": ("hive", "schema_updated"),
            "load": ("teradata", "loaded"),
            "full_load": ("teradata", "schema_updated")
}

AGE_OUT = {'load' : 2, 'merge' : 2, 'full_load' : 8, 'sqoop' : 48}

class EtlStatusDb(object):
    def __init__(self, dsn, table_limits_dsn = None):
        self.dsn = dsn
        self.table_limits_dsn = table_limits_dsn

    def __get_hostname():
        cmd = ["hostname", "-f"]
        (result, stdout, stderr) = os_util.execute_command(cmd)
        return stdout.strip()

    host_name = __get_hostname()

    def drop_db(self):
        try:
            sql = "drop table cdc_data_partitions"
            self._run_sql(sql)
            sql = "drop table etl_process_status"
            self._run_sql(sql)
            sql = "drop table table_replication_status"
            self._run_sql(sql)
        except:
            pass

    def clean_db(self):
        sql = "delete from cdc_data_partitions"
        self._run_sql(sql)
        sql = "delete from etl_process_status"
        self._run_sql(sql)
        sql = "delete from table_replication_status"
        self._run_sql(sql)

    def clear_table_metadata(self, service_name, hive_table_name, table_name):
        sql = "delete from cdc_data_partitions where host_name = '{self.host_name}' and service_name = '{service_name}' and table_name = '{hive_table_name}'".format(**locals())
        self._run_sql(sql)
        sql = "delete from etl_process_status where host_name = '{self.host_name}' and service_name = '{service_name}' and table_name = '{hive_table_name}'".format(**locals())
        self._run_sql(sql)
        sql = "delete from table_replication_status where host_name = '{self.host_name}' and service_name = '{service_name}' and table_name = '{table_name}'".format(**locals())
        self._run_sql(sql)

    def add_partitions(self, service_name, table_name, run_id, process_type, part_list):
        insert_list = []
        for part_name in part_list:
            insert_str = "( '{self.host_name}', '{service_name}', '{table_name}', '{part_name}', '{process_type}', '{run_id}')".format(**locals())
            insert_list.append(insert_str)
        insert_list_str = ",".join(insert_list)
        sql = """
                insert into cdc_data_partitions 
                    (host_name, service_name, table_name, part_name, process_type, run_id)
                    values  {insert_list_str}
                        on DUPLICATE KEY UPDATE run_id = '{run_id}' , attempt = attempt +1 
                    """.format(**locals())
        self._run_sql(sql)

    def relative_lag_check(self, service_name, table_name, process_type, source_max_pid):
        # Relative lag is based on the average time between sync cycles. So if the sync cycle is 2 hours and the computed lag (which is based on the primary key spread) is 4 hours, then the relative lag is 2.0
        initial_states = set(['full_load', 'sqoop'])
        incremental_states = {'load' : 'full_load', 'merge' : 'sqoop'}

        if process_type in initial_states:
            return 0

        initial_state = incremental_states[process_type]

        sql = """
                select process_type, max_pkid, max_src_pkid  from etl_process_status
                where host_name = '{self.host_name}' 
                      and service_name = '{service_name}'
                      and table_name = '{table_name}'
                      and process_type in ('{process_type}', '{initial_state}')
                      and status = 'SUCCESS'
                      ORDER BY update_time DESC LIMIT 4""".format(**locals())

        rows = [row for row in self._run_sql_fetchall(sql)]

        if len(rows) < 2:
            return 0

        min_target_pkid = None
        max_src_pkid = 0
        max_target_pkid = 0
        done = False
        for (ptype, max_pkid, max_src_pkid) in rows:
            if not done:
                max_src_pkid = int(max_src_pkid)
                if min_target_pkid is None or min_target_pkid > max_pkid:
                    min_target_pkid = max_pkid
                if max_pkid > max_target_pkid:
                    max_target_pkid = max_pkid
                done = (ptype == initial_states)

        avg_increment = float(max_target_pkid - min_target_pkid) / len(rows)
        relative_lag = ( (source_max_pid - max_target_pkid) / avg_increment) if avg_increment > 0 else 0

        return relative_lag

    def update_etl_status(self, data_src_platform, cluster_name, service_name, table_name, schema_only, table_only, target_table_name, run_id, process_type, status, cur_log_file, last_update_on, min_id, max_id, max_src_pkid, src_bounded_count, dest_bounded_count, bounded_cnt_failures, window_cnt_failures, min_window_pkid, src_window_count, dest_window_count, table_integrity_state, last_partition):
        if last_update_on == '0000-00-00 00:00:00':
            """ It considers several scenarios and computes the right last_update_on value when it is not available for the input data """ 
            proc_type = "'sqoop','merge'" if process_type in ('sqoop','merge') else "'full_load','load'"
            sql = """ select max(max_updated_on) from etl_process_status where host_name='{self.host_name}' and service_name='{service_name}' and table_name='{table_name}' and process_type in ({proc_type}) and status='SUCCESS'; """.format(**locals())
            run_time = '%s' % (datetime.strptime(run_id[len(run_id)-19:], "%Y-%m-%d-%H-%M-%S")) if status == "SUCCESS" else last_update_on
            """ uses run time only on success and when there is no update time associated with the data """ 
            for row in self._run_sql_fetchall(sql):
                last_update_on = row[0] if row[0] is not None else run_time 

        sql = """
                insert into etl_process_status 
                        (host_name, service_name, table_name, target_table_name, run_id, process_type, status, start_time, update_time, min_pkid, max_pkid, max_updated_on, max_src_pkid, src_bounded_count, dest_bounded_count, bounded_cnt_failures, window_cnt_failures, min_window_pkid, src_window_count, dest_window_count)
                    values ('{self.host_name}', '{service_name}', '{table_name}', '{target_table_name}', '{run_id}', '{process_type}',
                            '{status}', now(), now(), {min_id}, {max_id}, '{last_update_on}', {max_src_pkid}, {src_bounded_count}, {dest_bounded_count}, {bounded_cnt_failures}, {window_cnt_failures}, {min_window_pkid}, {src_window_count}, {dest_window_count})
                     ON DUPLICATE KEY UPDATE status = '{status}', update_time=NOW(), min_pkid={min_id}, max_pkid={max_id}, 
                                             max_updated_on = IF ('{last_update_on}' > max_updated_on, '{last_update_on}', max_updated_on), max_src_pkid = {max_src_pkid}, src_bounded_count = {src_bounded_count}, 
                                             dest_bounded_count = {dest_bounded_count}, bounded_cnt_failures = {bounded_cnt_failures}, window_cnt_failures = {window_cnt_failures}, src_window_count = {src_window_count}, dest_window_count = {dest_window_count}, min_window_pkid = {min_window_pkid}
                """.format(**locals())

        (result, sql_status, err_msg) = self._run_sql_with_retry(sql)
        if not sql_status:
            logging.exception("MANUAL INTERVENTION REQUIRED - {err_msg}: Manually set status to FAILED for service: {service_name}, table: {table_name}, process_type: {process_type}".format(**locals()))
            raise Exception("etl_process_status update failed: %s"%sql)

        (sink, event) = EVENTMAP[process_type]
        status = status.lower()

        on_dup_failed_attempts = "failed_attempts"
        on_insert_failed_attempts = "0"
        on_dup_log_file = "log_file"
        on_insert_log_file = ""
        max_sink_pkid = max_id
        on_dup_max_sink_pkid = "max_sink_pkid"
        if status == "failed":
            on_dup_failed_attempts = "failed_attempts + 1"
            on_insert_failed_attempts = "1"
            on_dup_log_file = "'%s'" % cur_log_file
            on_insert_log_file = cur_log_file
        elif status == "success":
            on_dup_failed_attempts = "0"
            on_insert_failed_attempts = "0"
            on_dup_log_file = "''"
            on_dup_max_sink_pkid = max_id
            
        
        relative_lag = self.relative_lag_check(service_name, table_name, process_type, max_src_pkid)
        
        sql = """
                INSERT into table_replication_status 
                        (host_name, service_name, table_name, sink, event, status, consistent_as_of, failed_attempts, table_integrity_state, log_file, max_src_pkid, max_sink_pkid, relative_lag)
                    values ('{self.host_name}', '{service_name}', '{table_only}', '{sink}', '{event}', '{status}', '{last_update_on}', {on_insert_failed_attempts}, '{table_integrity_state}', '{on_insert_log_file}', {max_src_pkid}, {max_sink_pkid}, {relative_lag})
                     ON DUPLICATE KEY UPDATE status = '{status}', consistent_as_of = IF ('{last_update_on}' > consistent_as_of, '{last_update_on}', consistent_as_of), failed_attempts = {on_dup_failed_attempts}, log_file = {on_dup_log_file}, 
                     table_integrity_state = '{table_integrity_state}', max_src_pkid = {max_src_pkid}, max_sink_pkid = {on_dup_max_sink_pkid}, relative_lag = {relative_lag}, event_time = now()
                """.format(**locals())
        self._run_sql(sql)

        if status == "success":
            self.update_measured_at_status(data_src_platform, cluster_name, service_name, schema_only, table_only, process_type, last_partition)

    def getPartitionId(self, last_partition):
        part_id = 0
        dr = last_partition.split("/")
        if len(dr) == 2:
            d = dr[0].split("=")[1]
            h = dr[1].split("=")[1]
            part_id = d.replace("-", "") + h + "00"
            part_id = int(part_id)
        return part_id

    def update_measured_at_status(self, data_src_platform, cluster_name, service_name, schema_only, table_only, process_type, last_partition):

        (sink_db, event) = EVENTMAP[process_type]
        part_id = self.getPartitionId(last_partition)
        sql = """
                UPDATE table_replication_status SET event_time = NOW() WHERE  host_name = '{self.host_name}' and service_name = '{service_name}' and sink = '{sink_db}' and table_name = '{table_only}' and event = '{event}'
                """.format(**locals())
        self._run_sql(sql)

        sql = """
                SELECT  MAX(consistent_as_of) as last_part  FROM table_replication_status WHERE  host_name = '{self.host_name}' and service_name = '{service_name}' and sink = '{sink_db}' and table_name = '{table_only}';
                """.format(**locals())

        lastmod = "0000-00-00 00:00:00"
        for row in self._run_sql_fetchall(sql):
            if (row[0] is not None and row[0] != lastmod): 
                lastmod = row[0] -  timedelta(minutes=30)
        
        if self.table_limits_dsn:
            full_cluster_name = sink_db if len(cluster_name) == 0 else "{cluster_name}_{sink_db}".format(**locals())
            sql = """
                    INSERT into table_limits 
                            (data_src_platform, process_name , environment , category, table_name , schema_name , consistent_upto , min_partition_id , max_partition_id , updated_at)
                        values ('{data_src_platform}', 'MEGATRON', '{full_cluster_name}' , '{service_name}', '{table_only}', '{schema_only}', '{lastmod}', '{part_id}', '{part_id}', now())
                         ON DUPLICATE KEY UPDATE consistent_upto = IF ('{lastmod}' > consistent_upto, '{lastmod}', consistent_upto), max_partition_id = '{part_id}', updated_at = NOW()
                    """.format(**locals())
                    
            (result, sql_status, err_msg) = self._run_sql_with_retry(sql, self.table_limits_dsn)
            if not sql_status:
                logging.exception("HARD SQL TABLE_LIMITS UPDATE FAILURE {err_msg}: {sql}".format(**locals()))
                raise Exception("table_limits update failed: %s"%sql)

    def get_last_op_time_str(self, service_name, table_name, process_type, status):
        dt = self.get_last_op_time(service_name, table_name, process_type, status)
        if dt:
            return dt.strftime("%Y-%m-%dT%H:%M:%S")
        return ""
    
    def recompute_bounded_counts(self, source_adaptor, destination_adaptor, run_id, service_name, table_name, process_type, min_id, max_id, bounded_cnt_failures, isWindow = False):
        src_bounded_count = source_adaptor.get_row_count(min_id, max_id)
        dest_bounded_count = destination_adaptor.get_row_count(min_id, max_id) 
        if isWindow:
           self.update_window_counts(run_id, service_name, table_name, process_type, min_id, src_bounded_count, dest_bounded_count) 
        else:
           self.update_increment_counts(run_id, service_name, table_name, process_type, src_bounded_count, dest_bounded_count) 
        return (src_bounded_count, dest_bounded_count)

    def update_increment_counts(self, run_id, service_name, table_name, process_type, src_bounded_count, dest_bounded_count):
        bounded_cnt_failures = "0" if src_bounded_count == dest_bounded_count else "bounded_cnt_failures + 1"        
        sql = "UPDATE etl_process_status SET bounded_cnt_failures = {bounded_cnt_failures}, src_bounded_count = {src_bounded_count}, dest_bounded_count = {dest_bounded_count} WHERE run_id = '{run_id}' and process_type = '{process_type}' and host_name = '{self.host_name}' and service_name = '{service_name}' and table_name = '{table_name}' ".format(**locals())
        logging.info("update_increment_counts: %s"%(sql))
        self._run_sql(sql)
        return (src_bounded_count <= dest_bounded_count)

    def update_window_counts(self, run_id, service_name, table_name, process_type, min_win_id, src_bounded_count, dest_bounded_count):
        bounded_cnt_failures = "0" if src_bounded_count == dest_bounded_count else "window_cnt_failures + 1"        
        sql = "UPDATE etl_process_status SET window_cnt_failures = {bounded_cnt_failures}, src_window_count = {src_bounded_count}, dest_window_count = {dest_bounded_count}, min_window_pkid = {min_win_id} WHERE run_id = '{run_id}' and process_type = '{process_type}' and host_name = '{self.host_name}' and service_name = '{service_name}' and table_name = '{table_name}' ".format(**locals())
        logging.info("update_window_counts: %s"%(sql))
        self._run_sql(sql)
        return (src_bounded_count <= dest_bounded_count)

    def get_integrity_count_ranges(self, max_id):
        min_id = max_id - BOUNDED_COUNT_RANGE
        if min_id < 0: min_id = 0 
    
        if min_id > 0:
            max_window_pkid = min_id
            min_window_pkid = max_window_pkid - BOUNDED_COUNT_RANGE
            if min_window_pkid <= 0:
                min_window_pkid = 0
            else:
                # ensure the window is spread effectively over the table range
                slots = min_window_pkid / BOUNDED_COUNT_RANGE
                slot = random.randint(0, slots)
                min_window_pkid = slot * BOUNDED_COUNT_RANGE
                max_window_pkid = min_window_pkid + BOUNDED_COUNT_RANGE
        else:
            (min_window_pkid, max_window_pkid) = (0,0)
        
        return (min_id, max_id, min_window_pkid, max_window_pkid)

    def get_last_success_increment_count_task(self, service_name, table_name):
        sql = """select run_id, min_pkid, max_pkid  from etl_process_status
                where host_name = '{self.host_name}' 
                      and service_name = '{service_name}'
                      and table_name = '{table_name}'
                      and process_type in ('sqoop', 'merge')
                      and status = 'SUCCESS'
                      and src_bounded_count = -1
                      ORDER BY start_time DESC LIMIT 1""".format(**locals())
        rows = [row for row in self._run_sql_fetchall(sql)]
        if len(rows) > 0:
            (run_id, min_pkid, max_pkid) = rows[0]
        else:
            (run_id, min_pkid, max_pkid) = (None,0,0)
            
        return (run_id, min_pkid, max_pkid)

    def get_recheck_increment_count_task(self, service_name, table_name, process_type):
        sql = """select run_id, min_pkid, max_pkid, bounded_cnt_failures  from etl_process_status
                where host_name = '{self.host_name}' 
                      and service_name = '{service_name}'
                      and table_name = '{table_name}'
                      and process_type = '{process_type}'
                      and status = 'SUCCESS'
                      and bounded_cnt_failures > 0
                      ORDER BY bounded_cnt_failures DESC LIMIT 1""".format(**locals())
        rows = [row for row in self._run_sql_fetchall(sql)]
        if len(rows) > 0:
            (run_id, min_pkid, max_pkid, cnt_failures) = rows[0]
        else:
            (run_id, min_pkid, max_pkid, cnt_failures) = ("",0,0,0)
            
        return (run_id, min_pkid, max_pkid, cnt_failures)

    def get_recheck_random_window_count_task(self, service_name, table_name, process_type):
        sql = """select run_id, min_window_pkid, max_pkid, window_cnt_failures  from etl_process_status
                 where host_name = '{self.host_name}' 
                      and service_name = '{service_name}'
                      and table_name = '{table_name}'
                      and process_type = '{process_type}'
                      and status = 'SUCCESS'
                      and window_cnt_failures > 0
                      ORDER BY window_cnt_failures DESC LIMIT 1""".format(**locals())
        rows = [row for row in self._run_sql_fetchall(sql)]
        if len(rows) > 0:
            (run_id, min_window_pkid, max_pkid, cnt_failures) = rows[0]
            (min_increment_pkid, max_increment_pkid, min_not_used, max_not_used) = self.get_integrity_count_ranges(max_pkid)
            max_window_pkid = int(min_window_pkid) + BOUNDED_COUNT_RANGE
            if max_window_pkid > min_increment_pkid:
                max_window_pkid = min_increment_pkid
        else:
            (run_id, min_window_pkid, max_window_pkid, cnt_failures) = ("",0,0,0)
        return (run_id, min_window_pkid, max_window_pkid, cnt_failures)

    def recheck_data_integrity_count_failures(self, source_adaptor, destination_adaptor, cur_run_id, service_name, table_name, process_type):
        # For loads (teradata) perform the actual select count(*) at this point. For merge (hive) get the count task descriptor string
        table_online_state = "SUCCESS"
        conf = config.get_conf()
        max_failures = int(conf.get('max_failures', 2))
        
        # INCREMENT CHECK: Find the incident record containing the maximum number of increment failures
        (run_id, min_pkid, max_pkid, cnt_failures) = self.get_recheck_increment_count_task(service_name, table_name, process_type)
        if cnt_failures > 0:
            (src_bounded_count, dest_bounded_count) = self.recompute_bounded_counts(source_adaptor, destination_adaptor, run_id, service_name, table_name, process_type, min_pkid, max_pkid, cnt_failures)
            if src_bounded_count > dest_bounded_count:
                table_online_state = "FAILURE:INCREMENTAL_COUNT"
        
        # RANDOMIZED WINDOW CHECK: Find the incident record containing the maximum number of randomized window count failures
        (run_id, min_pkid, max_pkid, cnt_failures) = self.get_recheck_random_window_count_task(service_name, table_name, process_type)
        if cnt_failures > 0:                
            (src_bounded_count, dest_bounded_count) = self.recompute_bounded_counts(source_adaptor, destination_adaptor, run_id, service_name, table_name, process_type, min_pkid, max_pkid, cnt_failures, isWindow=True)
            if src_bounded_count > dest_bounded_count:
                table_online_state = "FAILURE:WINDOW_COUNT"

        return table_online_state
        
    def get_last_op_time(self, service_name, table_name, process_type, status):
        sql = """
                select max(start_time)  from etl_process_status
                where host_name = '{self.host_name}' 
                      and service_name = '{service_name}'
                      and table_name = '{table_name}'
                      and process_type = '{process_type}'
                      and status = '{status}' """.format(**locals())
        result = self._run_sql(sql)
        return result[0] if result else None

    def get_last_run_id(self, service_name, table_name, process_type, status):
        sql = """
                select  run_id from etl_process_status
                where host_name = '{self.host_name}' 
                      and service_name = '{service_name}'
                      and table_name = '{table_name}'
                      and process_type = '{process_type}'
                      and status = '{status}' order by update_time DESC limit 1  """.format(**locals())
        result = self._run_sql(sql)
        return result[0] if result else None

    def get_last_sqoop_run_id(self, service_name, table_name):
        return self.get_last_run_id(service_name, table_name, "sqoop", "SUCCESS")

    def is_any_have_status(self, service_name, table_name, process_type, status):
        sql = """
                select count(1)  from etl_process_status
                where host_name = '{self.host_name}' 
                      and service_name = '{service_name}'
                      and table_name = '{table_name}'
                      and process_type in ({process_type})
                      and status = '{status}'""".format(**locals())
        result = self._run_sql(sql)
        return result[0] > 0

    def get_last_success_part(self, service_name, table_name, process_type):
        sql = """
                select concat('ds=',substr(substr(max(b.run_id),-19),1,10),'/hr=',substr(substr(max(b.run_id),-19),12,2)) from 
                        ( select run_id, part_name from cdc_data_partitions 
                          where host_name = '{self.host_name}' and service_name = '{service_name}' 
                                and table_name = '{table_name}' and process_type = '{process_type}' ) a
                        join 
                        ( select run_id from etl_process_status 
                          where host_name = '{self.host_name}' 
                                and service_name = '{service_name}' 
                                and table_name = '{table_name}' 
                                and process_type = '{process_type}'
                                and status = 'SUCCESS'
                                ) b
                        on 
                           ( a.run_id = b.run_id ) 

                """.format(**locals())
        result = self._run_sql(sql)
        return result[0] if result else None

    def get_all_inst(self, service_name, status, date_id):
        sql = """
                select table_name, process_type, run_id  from etl_process_status
                where host_name = '{self.host_name}' and service_name = '{service_name}'
                      and status = '{status}' and update_time > '{date_id}'""".format(**locals())
        return self._run_sql_fetchall(sql)

    def get_failed_parts_for_run_id(self, service_name, table_name, run_id):
        sql = """
                select part_name, attempt 
                    from cdc_data_partitions
                    where host_name = '{self.host_name}' 
                        and service_name = '{service_name}'
                        and table_name = '{table_name}'
                        and run_id = '{run_id}'""". format(**locals())

        return self._run_sql_fetchall(sql)

    def get_purge_list(self, service_name, purge_age_hr):
        sql = """
                select hdfs_path 
                    from hdfs_trash
                    where host_name = '{self.host_name}' and service_name = '{service_name}'
                          and HOUR(TIMEDIFF(NOW(), trash_date))>{purge_age_hr}""".format(**locals())

        return [row[0] for row in self._run_sql_fetchall(sql)]

    def get_failed_parts(self, service_name, table_name):
        sql = """
                select a.part_name from
                        ( select part_name from cdc_data_partitions
                          where host_name = '{self.host_name}' 
                                and service_name = '{service_name}'
                                and table_name = '{table_name}' ) a
                        join
                        ( select run_id from etl_process_status
                          where host_name = '{self.host_name}' 
                                and service_name = '{service_name}'
                                and table_name = '{table_name}'
                                and process_type = 'merge'
                                and status = 'FAILED'
                                ) b
                        on
                           ( a.run_id = b.run_id )

                """.format(**locals())
        result = self._run_sql_fetachall(sql)
            
        return [ row[0] for row in result ]

    def _run_sql_fetch_private(self, sql, alt_dsn = None, fetch_all = False) :
        result = None
        dsn = alt_dsn if alt_dsn != None else self.dsn
        connection=SQL(dsn=dsn,  adapter="MySQLDirect")
        logging.info("Running SQL[%s]: %s"%(dsn, sql))
        with connection.get_cursor() as cursor:
            cursor.execute(sql)
            result = cursor.fetchall() if fetch_all else cursor.fetchone()
        return result

    def _run_sql_with_retry(self, sql, alt_dsn = None, fetch_all = False, retries = 3, sleep_seconds = 60):
        done = False
        success = True
        result  = None
        err_msg = ''
        while not done:
            try:
               result = self._run_sql_fetch_private(sql, alt_dsn, fetch_all) 
               done = True
            except Exception as e:
                err_msg = str(e)
                logging.warn("SQL error: {err_msg}".format(**locals()))
                if retries > 0:
                    retries -= 1
                    logging.info("Retrying query in %d seconds...." % sleep_seconds)
                    time.sleep(sleep_seconds)
                else:
                    success = False
                    done = True
        return (result, success, err_msg)

    def _run_sql(self, sql, alt_dsn = None, retries = 3, sleep_seconds = 60) :
        (result, success, err_msg) = self._run_sql_with_retry(sql, alt_dsn, fetch_all = False, retries = retries, sleep_seconds = sleep_seconds)
        if not success:
            logging.exception("HARD SQL METASTORE FAILURE {err_msg}: {sql}".format(**locals()))
            raise Exception("EtlStatusDb query failed: %s"%sql)
        return result

    def _run_sql_fetchall(self, sql, alt_dsn = None, retries = 3, sleep_seconds = 60) :
        (result, success, err_msg) = self._run_sql_with_retry(sql, alt_dsn, fetch_all = True, retries = retries, sleep_seconds = sleep_seconds)
        if not success:
            logging.exception("HARD SQL METASTORE FAILURE {err_msg}: {sql}".format(**locals()))
            raise Exception("EtlStatusDb query failed: %s"%sql)
        return result

def test_td_load():
    # service_name, tab_name, tab_only, run_id,  mode, status, last_update
    etl_db = EtlStatusDb('tungsten_etl_status_dev')
    etl_db.clean_db()
    etl_db.add_partitions('test_service', 'test_table', [ 'ds=2014-01-10/hr=01', 'ds=2014-01-10/hr=02'], 'run_id1')
    etl_db.add_partitions('test_service', 'test_table', [ 'ds=2014-01-10/hr=01', 'ds=2014-01-10/hr=03'], 'run_id2')

    etl_db.update_diff_tab('test_service', 'test_table', 'ds1', 'ts1', 'run_id1' )
    etl_db.update_diff_tab('test_service', 'test_table', 'ds2', 'ts2', 'run_id2' )
    print `etl_db.partitions_not_loaded('test_service', 'test_table')`
    etl_db.add_load_partitions('test_service', 'test_table',  'run_id3', [ ('ds1', 'ts1') ])

    print `etl_db.partitions_not_loaded('test_service', 'test_table')`
        
if __name__ == '__main__' :   
    pass
