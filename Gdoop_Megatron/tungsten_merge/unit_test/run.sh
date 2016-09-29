sqoop_file=${1}
change_file=$2
final_file=$3
case=$4
_base_dir=/tmp/tungsten_unit_test
hadoop dfs -rm -r -skipTrash $_base_dir
sqoop_tab="tmpsa_tung_test_sqoop"_${case}
change_tab="tmpsa_tung_test_change"_${case}
final_tab="tmpsa_tung_test_final"_${case}
hadoop dfs -mkdir $_base_dir/$sqoop_tab
hadoop dfs -mkdir $_base_dir/$change_tab
hadoop dfs -mkdir $_base_dir/$final_tab
hadoop dfs -put $sqoop_file $_base_dir/$sqoop_tab
hadoop dfs -put $change_file $_base_dir/$change_tab
hive -e "drop table if exists $sqoop_tab; create external table $sqoop_tab (id string, col1 string, col2 string, col3 string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' location '$_base_dir/$sqoop_tab'"
hive -e "drop table if exists $change_tab; create external table $change_tab (tungsten_opcode string, tungsten_seqno bigint, tungsten_rowid bigint, tungsten_time string,id string, col1 string, col2 string, col3 string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' location '$_base_dir/$change_tab'"
hive -e "drop table if exists $final_tab; create external table $final_tab (id string, col1 string, col2 string, col3 string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' location '$_base_dir/$final_tab'"

hive -e "\
add file ..//apply_changes.py;\
                      from ( from ( \
                            select 'T' as tungsten_opcode , -1 as tungsten_seqno , -1 as tungsten_rowid, '1111-11-11 11:11:11' as tungsten_time,\
                                          id ,col1,col2,col3 from $sqoop_tab\
                            union all\
                            select tungsten_opcode, tungsten_seqno, tungsten_rowid, tungsten_time,\
                                     id, col1,col2,col3 from $change_tab\
                        ) map1\
                      select * \
                        distribute by id sort by id, tungsten_seqno, tungsten_rowid ) sorted1\
                  insert overwrite table $final_tab\
                    select TRANSFORM(*)\
                            ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'\
                           USING 'python apply_changes.py id,col1,col2,col3 id'\
                    as id,col1,col2,col3 ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';"

hive -e "select * from $final_tab"|sort|sed 's/	/,/g' > /tmp/$final_tab
cat $final_file | sort > /tmp/${final_tab}_org
diff -u /tmp/$final_tab /tmp/${final_tab}_org
ret=$?
if [ $ret -ne 0 ]; then
    echo "Failed"
fi
