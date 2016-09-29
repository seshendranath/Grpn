<%
    import time
    max_inserter_size = 1000000
    # explicitly initializing needed locals here to get "format(**locals())" to work correctly
    loc_ = loc
    service_name_ = service_name
    schema_ = schema
    tab_ = tab
    workflow_id_ = workflow_id
        
    final_table_name = "{schema_}_{tab_}_final".format(**locals())
    timestamp = int(time.time())
    final_backup_table_name = "{final_table_name}_{timestamp}".format(**locals())
    current_tab_name = "{final_table_name}_current".format(**locals())
    diff_table_name = "{final_table_name}_diff".format(**locals())
    loc_final_tab = "{loc_}/{service_name_}/{schema_}/{tab_}/{schema_}_{tab_}_{workflow_id_}".format(**locals())
     
    # hive
    keys = [ akey.strip() for akey in key_str.split(",") ]
%>
        
    settings: 
        parallelism: ${parallelism}
        wf_cleanup: 0

    start_task:
        class: NopTask


    add_partition_task:
        class: HiveTask
        dependencies: 
          - start_task
        configuration: 
          hql: ${add_partition_sql}


    create_staging_htable:
      class: HiveTask
      dependencies: 
          - add_partition_task
      configuration: 
        hql:
          - USE ${hive_schema};
          -  
            CREATE EXTERNAL TABLE ${current_tab_name} (
               ${col_def_str} 
            ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '${delimiter_hive}' location '${loc_final_tab}';


    merge_task:
      class: HiveTask
      dependencies: 
            - create_staging_htable
      configuration: 
        hql: 
            - set hive.exec.compress.output=false;${hive_set}
            - dfs -mkdir -p ${hdfs_temp_loc}
            - dfs -chmod 777 ${hdfs_temp_loc}
            - use ${hive_schema};
            - set mapred.job.name=MEGATRON_MERGE_${final_table_name}_${workflow_id};
                  add file ${script_dir}/apply_changes.py;
                  
                  from (
                    from (
                        select 'T' as tungsten_opcode , -1 as tungsten_seqno ,
                                   -1 as tungsten_rowid, '1111-11-11 11:11:11' as tungsten_time,
                                      ${col_str} from ${final_table_name}
                        union all
                        select tungsten_opcode, tungsten_seqno, tungsten_rowid, tungsten_time,
                                 ${col_str} from ${stg_tab} where ${where_stmt}
                    ) map1
                  select *
                      distribute by ${key_str} sort by ${key_str}, tungsten_seqno, tungsten_rowid ) sorted1
                        
                      insert overwrite table ${current_tab_name}
                        select TRANSFORM(*)
                                ROW FORMAT DELIMITED FIELDS TERMINATED BY '${delimiter_hive}'
                               USING '/usr/local/bin/python apply_changes.py ${delimiter_hex_str} merge ${col_arg_str} ${key_str} ${data_filters} ${hdfs_temp_loc} ${count_tasks}'
                        as ${col_str} ROW FORMAT DELIMITED FIELDS TERMINATED BY '${delimiter_hive}';
                    
                alter table ${final_table_name} rename to ${final_backup_table_name};
                alter table ${current_tab_name} rename to ${final_table_name};
                drop table ${final_backup_table_name};

% if hdfs_cur_extern_loc:
    add_to_trash:
      class: SQLExecute
      dependencies: 
            - merge_task
      configuration: 
        db:
          adapter: MySQLDirect
          dsn: ${etl_status_dsn}
        sql: 
            - INSERT IGNORE INTO hdfs_trash (host_name, service_name, table_name, hdfs_path) VALUES ('${host_name}', '${service_name}', '${tab}', '${hdfs_cur_extern_loc}');
% endif


% if len(purge_list) > 0:
    <% 
       hdfs_str_list = "'" + "', '".join(purge_list) + "'"
    %>
    purge_task:
      class: HDFSTask
      dependencies: 
        - merge_task
      configuration: 
          work:
             try:
                rm -R:
                  - -skipTrash
                  % for hdfs_loc in purge_list:
                     ${hdfs_loc}
                  % endfor


    update_purge_list:
      class: SQLExecute
      dependencies: 
            - purge_task
      configuration: 
        db:
          adapter: MySQLDirect
          dsn: ${etl_status_dsn}
        sql: 
            - DELETE FROM hdfs_trash WHERE service_name = '${service_name}' AND hdfs_path IN (${hdfs_str_list});

% endif
