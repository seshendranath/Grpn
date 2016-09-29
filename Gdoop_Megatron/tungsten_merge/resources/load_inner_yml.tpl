<%
    teradata_table_qualified = "%s.%s"%(teradata_final_schema, teradata_table)   
    
    teradata_diff_table = teradata_table[:-3] if 27 < len(teradata_table) else teradata_table
    teradata_diff_table += "_DF"
    teradata_diff_table_qualified = "%s.%s"%(teradata_staging_schema, teradata_diff_table)       
        
    # Teradata Specific
    # constuct merge statement components
    td_keys = [ akey.strip() for akey in td_key_str.split(",") ]
    td_key_pairs = ["t.%s=d.%s"%(akey, akey) for akey in td_keys]

    td_columns = [ acol.strip() for acol in td_col_str.split(",") ]
    on_expression = ' AND '.join(td_key_pairs)
    none_key_columns = [ acol for acol in td_columns if acol not in td_keys ]
    set_pairs = ["%s=d.%s"%(acol, acol) for acol in none_key_columns]
    update_expression = ', '.join(set_pairs)
    td_column_names_expression = ', '.join(td_columns)
    insert_vals = ["d.%s"% acol for acol in td_columns]
    insert_vals_expression = ', '.join(insert_vals)
    
%>
    drop_diff_${target_name}:
      class: SQLExecute
      dependencies: 
          - ${first_dependent}
      settings:
        try: 1
      configuration:
        db:
          adapter: Teradata
          dsn: ${teradata_dsn}
        sql:
          - DROP TABLE ${teradata_diff_table_qualified};

    create_td_diff_table_${target_name}:
      class: SQLExecute
      dependencies: 
          - drop_diff_${target_name}
      configuration:
        db:
          adapter: Teradata
          dsn: ${teradata_dsn}
        sql:
          - create table ${teradata_diff_table_qualified} AS ${teradata_table_qualified} with no data;
          - ALTER TABLE ${teradata_diff_table_qualified} ADD db_op CHAR(1) CHARACTER SET UNICODE CASESPECIFIC, ADD db_op_date VARCHAR(24) CHARACTER SET LATIN; 
        teardown_sql:
          - DROP TABLE ${teradata_diff_table_qualified};

    smart_load_td_diff_table_${target_name}:
      class: SQLLoadFromHDFS
      dependencies: 
          - create_td_diff_table_${target_name}
      configuration:
        db:
          adapter: Teradata
          dsn: ${teradata_dsn}
        source_path: ${loc_diff_file}/*
        destination_table: ${teradata_diff_table_qualified}
        skip_is_running_check: True
        load_options:
            replace_with_null: \N
            skip_columns: ${skip_columns}
            drop_target_before_load: Y
            fastload: Y
            error_limit: 10000000
            tpt_reader.options:
               TruncateColumnData: Y
               TextDelimiterHex: '${delimiter_hex_str}'
               
    apply_diffs_${target_name}:
      class: SQLExecute
      dependencies: 
          - smart_load_td_diff_table_${target_name}
      configuration:
        db:
          adapter: Teradata
          dsn: ${teradata_dsn}
        sql: 
          - DELETE FROM ${teradata_table_qualified} WHERE (${td_key_str}) IN (SELECT ${td_key_str} from ${teradata_diff_table_qualified} WHERE db_op = 'D');
          - MERGE INTO ${teradata_table_qualified} AS t
            USING (SELECT ${td_column_names_expression} FROM ${teradata_diff_table_qualified} WHERE db_op = 'M')   AS d
            ON (${on_expression})
            WHEN MATCHED THEN
              UPDATE SET ${update_expression}
            WHEN NOT MATCHED THEN 
              INSERT (${td_column_names_expression})
              VALUES (${insert_vals_expression});

