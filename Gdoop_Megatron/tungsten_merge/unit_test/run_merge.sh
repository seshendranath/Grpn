#!/bin/sh
ds=`date "+%Y-%m-%d"`
hh=`date "+%H%M%S"`
run_id="unit_test_${ds}-${hh}"

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

export PYTHONPATH=/usr/local/lib/python2.7/site-packages/zombie_runner
export ZOMBIERC=$bin/tungsten_merge_zrc

data_dir=${1}
export _TEST_SHCEMA_FILE=$data_dir/schema_file1
sqoop_file=$data_dir/sqoop_file1
change_file=$data_dir/change_file1
final_file=$data_dir/final_file1
case=$1
hive_schema=tungsten_unit_test
_base_dir=/user/tungsten/unit_test
schema_name=test1_schema
td_schema=sandbox

tab_name=test1_table
stage_location=$_base_dir/staging/test1

cat $change_file | sed 's/,//g' > /tmp/change_file1
cat $sqoop_file | sed 's/,/	/g' > /tmp/sqoop_file1
hadoop dfs -rm -r -skipTrash $stage_location $_base_dir/final
hadoop dfs -mkdir -p $stage_location/$schema_name/$tab_name
hadoop dfs -mkdir -p $stage_location/$schema_name/$tab_name/ds=2001-01-01/hr=01
hadoop dfs -mkdir -p $stage_location/$schema_name/$tab_name/ds=2001-01-01/hr=02
hadoop dfs -put /tmp/change_file1 $stage_location/$schema_name/$tab_name/ds=2001-01-01/hr=01/
hadoop dfs -put /tmp/change_file1 $stage_location/$schema_name/$tab_name/ds=2001-01-01/hr=02/
hadoop dfs -mkdir -p $_base_dir/final/sqoop_tab
hadoop dfs -mkdir -p $_base_dir/final/test1_schema_test1_table_run_id1_sqoop
hadoop dfs -put /tmp/sqoop_file1 $_base_dir/final/sqoop_tab
hadoop dfs -put /tmp/sqoop_file1 $_base_dir/final/test1_schema_test1_table_run_id1_sqoop
final_tab=${schema_name}_${tab_name}
#{loc}/{schema}_{tab}_{workflow_id}_sqoop"

hive -e "use ${hive_schema}; drop table ${final_tab}_stg;"
hive -e "use ${hive_schema}; drop table ${final_tab}_final; create external table ${final_tab}_final  ( id string, col1 string, col2 string, col3 string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'  location '$_base_dir/final/sqoop_tab'"

#cmd="zombie_runner run $bin/resources/init --context=source_data_loc:$_base_dir/final/sqoop_tab,schema_name:${td_schema},target_table:${tab_name}"
#$cmd
#ret=$?
#if [ $ret -ne 0 ]; then
#   echo "Failed"
#   exit
#fi

echo "delete from cdc_data_partitions where service_name ='test1';" | mysql -h localhost -u tungsten_etl -ppass tungsten_etl_status_dev 
echo "delete from etl_process_status where service_name ='test1';" | mysql -h localhost -u tungsten_etl -ppass tungsten_etl_status_dev 
echo "insert into etl_process_status (service_name, table_name, run_id, process_type, status, update_time) values('test1', 'test1_schema_test1_table', 'run_id1', 'sqoop', 'SUCCESS', now() - interval 1 day)" |  mysql -h localhost -u tungsten_etl -ppass tungsten_etl_status_dev

cmd="python $bin/../merge_tab.py --test_mode --run_mode=merge  --tab=${schema_name}.${tab_name} --tungsten_conf=$data_dir/tung_test.yml --db_conf=$data_dir/mysql_gp_test.yml --workflow_id=${run_id} -vv"
$cmd
ret=$?

#cmd="zombie_runner run $bin/resources/dump --context=schema_name:${td_schema},target_table:${tab_name} "
#$cmd
#ret_d=$?
#if [ $ret_d -ne 0 ]; then
#   echo "Failed"
#   exit
#fi

hive -e "use ${hive_schema}; select * from ${final_tab}_final"|sort|sed 's/	/,/g' > /tmp/$final_tab
cat $final_file | sort > /tmp/${final_tab}_baseline
diff -u /tmp/$final_tab /tmp/${final_tab}_baseline
ret=$?
if [ $ret -ne 0 ]; then
    echo "Failed"
fi

#sed -i 's/\r//g' /tmp/tungsten/sandbox.${tab_name}_final.dat
#diff -u /tmp/${final_tab}_baseline /tmp/tungsten/sandbox.${tab_name}_final.dat
#ret=$?

#if [ $ret -ne 0 ]; then
#    echo "Teradata Merge Table Compare Failed: /tmp/tungsten/sandbox.${tab_name}_final.dat"
#fi