<%
    loc_ = loc
    schema_ = schema
    tab_ = tab
    workflow_id_ = workflow_id
        
    final_table_name = "{schema_}_{tab_}_final".format(**locals())
    current_tab_name = "{final_table_name}_current".format(**locals())
    diff_table_name = "{final_table_name}_diff".format(**locals())
    loc_final_tab = "{loc_}/{schema_}_{tab_}_{workflow_id_}".format(**locals())
            
    # hive
    keys = [ akey.strip() for akey in key_str.split(",") ]
        
%>        
    settings: 
        parallelism: ${parallelism}
        sqlload.optimize_operator: True
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
          - DROP TABLE if exists ${diff_table_name};
          -
            CREATE EXTERNAL TABLE IF NOT EXISTS ${diff_table_name} (
               ${col_def_str},
               op string, op_date string
            ) PARTITIONED BY(ds string, ts string)  ROW FORMAT DELIMITED FIELDS TERMINATED BY '${delimiter_hive}'  LOCATION '${loc_diff_tab}';

    add_diff_partition:
      class: HiveTask
      dependencies:
          - create_staging_htable
      configuration:
        hql:
          - USE ${hive_schema};
          - ALTER TABLE ${diff_table_name} ADD IF NOT EXISTS PARTITION (ds='${ds}', ts='${ts}') LOCATION '${loc_diff_tab}/ds=${ds}/ts=${ts}';


    merge_task:
      class: HiveTask
      dependencies:
            - add_diff_partition
      configuration:
        hql:
            - set hive.exec.compress.output=false;${hive_set}
            - dfs -mkdir ${hdfs_temp_loc}
            - dfs -chmod 777 ${hdfs_temp_loc}
            - use ${hive_schema};
            - set mapred.job.name=MEGATRON_MERGE_${final_table_name}_${workflow_id};
                  add file ${script_dir}/apply_changes.py;

                  from (
                    from (
                        select tungsten_opcode, tungsten_seqno, tungsten_rowid, tungsten_time,
                                 ${col_str} from ${stg_tab} where ${where_stmt}
                    ) map1
                  select *
                      distribute by ${key_str} sort by ${key_str}, tungsten_seqno, tungsten_rowid ) sorted1

                      insert overwrite table ${diff_table_name} PARTITION(ds='${ds}', ts='${ts}')
                        select TRANSFORM(*)
                            ROW FORMAT DELIMITED FIELDS TERMINATED BY '${delimiter_hive}'
                            USING '/usr/local/bin/python apply_changes.py ${delimiter_hex_str}  diff ${col_arg_str} ${key_str} ${data_filters} ${hdfs_temp_loc}'
                        as ${col_str},op,op_date ROW FORMAT DELIMITED FIELDS TERMINATED BY '${delimiter_hive}';


    ${load_inner_yml}

    drop_diff_table:
      class: HiveTask
      dependencies:
      % for tname in target_names:
          - apply_diffs_${tname}
      % endfor
      configuration:
        hql:
          - USE ${hive_schema};
          - DROP TABLE if exists ${diff_table_name};
    
    delete_diff_files:
      class: HDFSTask
      dependencies:
             - drop_diff_table 
      configuration:
        work:
            try:
                rm -R:
                  - -skipTrash
                    ${loc_diff_file}
