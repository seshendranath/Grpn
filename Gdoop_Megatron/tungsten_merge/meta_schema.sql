-- Megatron's Metastore

--use mysql;
--INSERT INTO user VALUES('localhost','megatron',PASSWORD('megatron'), 'Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y', 'Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y', '','','','',0,0,0,0,NULL,NULL);
--INSERT INTO user VALUES('%','megatron',PASSWORD('megatron'), 'Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y', 'Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y', '','','','',0,0,0,0,NULL,NULL);

--FLUSH PRIVILEGES;

USE megatron_production;

DROP TABLE IF EXISTS cdc_data_partitions;
        CREATE TABLE cdc_data_partitions(
                        host_name VARCHAR(128) NOT NULL, 
                        service_name VARCHAR(128) NOT NULL, 
                        table_name VARCHAR(128) NOT NULL, 
                        part_name VARCHAR(64) NOT NULL, 
                        run_id VARCHAR(128) NOT NULL, 
                        attempt int default 1,
                        process_type VARCHAR(32) NOT NULL,
                        update_time timestamp DEFAULT CURRENT_TIMESTAMP, 
                    PRIMARY KEY (host_name, service_name, table_name,part_name, process_type));

DROP TABLE IF EXISTS etl_process_status;
        create table etl_process_status(
                        host_name VARCHAR(128) NOT NULL, 
                        service_name VARCHAR(128) NOT NULL, 
                        table_name VARCHAR(128) NOT NULL, 
                        run_id VARCHAR(128) NOT NULL, 
                        process_type VARCHAR(64) NOT NULL,
                        target_table_name    VARCHAR(128) NOT NULL COMMENT 'The table name on the target',
                        status VARCHAR(64) NOT NULL,
                        start_time timestamp DEFAULT CURRENT_TIMESTAMP,
                        update_time datetime,
                        min_pkid int DEFAULT 0, 
                        max_pkid int DEFAULT 0, 
                        max_updated_on datetime DEFAULT '1970-01-01',
                        max_src_pkid int DEFAULT 0, 
                        src_bounded_count int DEFAULT 0,
                        dest_bounded_count int DEFAULT 0, 
                        bounded_cnt_failures int DEFAULT 0, 
                        window_cnt_failures  int DEFAULT 0, 
                        min_window_pkid int DEFAULT 0, 
                        src_window_count int DEFAULT 0, 
                        dest_window_count int DEFAULT 0, 
                        PRIMARY KEY(host_name, service_name,table_name,run_id, process_type));      
                                            
-- ALTER TABLE etl_process_status ADD COLUMN window_cnt_failures int DEFAULT 0 after bounded_cnt_failures;
-- ALTER TABLE etl_process_status ADD COLUMN min_window_pkid int DEFAULT 0 after window_cnt_failures;

DROP TABLE IF EXISTS table_replication_status;
CREATE TABLE table_replication_status (
                host_name VARCHAR(128) NOT NULL, 
                service_name  VARCHAR(128) NOT NULL,
                table_name    VARCHAR(128) NOT NULL,
                sink          ENUM('hive', 'teradata', 'spark', 'hbase'),
                event         ENUM('schema_updated', 'loaded'),
                consistent_as_of  DATETIME DEFAULT '1970-01-01',
                status          ENUM('success', 'not_needed', 'running', 'failed', 'already_running'),
                event_time      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                failed_attempts int default 0,
                table_integrity_state VARCHAR(64) DEFAULT 'SUCCESS',
                log_file        VARCHAR(2048) DEFAULT NULL,
                max_src_pkid int DEFAULT 0, 
                max_sink_pkid int DEFAULT 0, 
                relative_lag    float DEFAULT 0,
            PRIMARY KEY (host_name, service_name, table_name, sink, event));

-- ALTER TABLE table_replication_status ADD COLUMN table_integrity_state ENUM('SUCCESS', 'FAILURE') DEFAULT 'SUCCESS' after failed_attempts;

DROP TABLE IF EXISTS hdfs_trash;
CREATE TABLE hdfs_trash (
    host_name VARCHAR(128) NOT NULL,
    service_name  VARCHAR(128) NOT NULL,
    table_name    VARCHAR(128) NOT NULL,
    hdfs_path     VARCHAR(2048) NOT NULL,
    trash_date    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
INDEX (host_name, service_name, hdfs_path));


DROP TABLE IF EXISTS service_state;
CREATE TABLE service_state (
    host_name VARCHAR(128) NOT NULL,
    service_name    VARCHAR(128) NOT NULL,
    latency         FLOAT default 0,
    tungsten_state  VARCHAR(64) default 0,
    modified_on     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
PRIMARY KEY (host_name, service_name));


DROP VIEW IF EXISTS megatron_services;
CREATE VIEW megatron_services AS SELECT host_name, service_name, CONVERT(latency/60, UNSIGNED) AS service_latency, tungsten_state, 
                   TIMESTAMPDIFF(MINUTE, modified_on, NOW()) AS cron_latency,
                   IF(TIMESTAMPDIFF(MINUTE, modified_on, NOW()) > 30, 'REFRESH_CRON:ERROR', IF (tungsten_state <> 'ONLINE', 'OFFLINE:ERROR', 'ONLINE')) AS status
                   FROM service_state WHERE tungsten_state != 'OFFLINE:NORMAL';

DROP VIEW IF EXISTS megatron_table_status;
CREATE VIEW megatron_table_status AS SELECT host_name, service_name, table_name, sink, event, consistent_as_of,  
                  relative_lag, failed_attempts,
                  IF(failed_attempts >= 2, 'FAILURE:PROCESS_ERROR', IF(table_integrity_state != 'SUCCESS', table_integrity_state, IF(relative_lag >= 3.0, 'FAILURE:HIGH_LATENCY', 'ONLINE'))) AS status, 
                  event_time,
                  log_file
                  FROM table_replication_status;

DROP VIEW IF EXISTS megatron_table_errors;
CREATE VIEW megatron_table_errors AS SELECT host_name, service_name, table_name, max(consistent_as_of) AS consistent_as_of, ROUND(max(relative_lag), 2) as relative_latency,  MAX(event_time) AS event_time, max(log_file) as log_file, max(status) as status
                FROM megatron_table_status where status != 'ONLINE'
                group by host_name, service_name, table_name;

DROP VIEW IF EXISTS megatron_table_online;
CREATE VIEW megatron_table_online AS SELECT s.host_name host_name, s.service_name service_name, s.table_name table_name, max(s.consistent_as_of) AS consistent_as_of, ROUND(max(s.relative_lag), 2) as relative_latency,  MAX(s.event_time) AS event_time, max(s.log_file) as log_file, max(s.status) as status
                FROM megatron_table_status s LEFT OUTER JOIN megatron_table_errors ON s.host_name = megatron_table_errors.host_name and s.service_name = megatron_table_errors.service_name and s.table_name = megatron_table_errors.table_name
                WHERE megatron_table_errors.table_name is null
                group by s.host_name, s.service_name, s.table_name;

DROP VIEW IF EXISTS megatron_tables;
CREATE VIEW megatron_tables AS 
                SELECT host_name, service_name, table_name, consistent_as_of, relative_latency, event_time, log_file, status from megatron_table_errors
                UNION
                SELECT host_name, service_name, table_name, consistent_as_of, relative_latency, event_time, log_file, status from megatron_table_online;
