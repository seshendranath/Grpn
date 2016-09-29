#!/bin/bash
hive -e "use tungsten_gp; show tables '*_final'" > /tmp/final_table_list

hadoop dfs -ls -d /user/tungsten/final_groupon_production/*_sqoop_*_sqoop | grep -v "Found " | awk '{print $(NF)}' > /tmp/dir_list

while read tab; do
    tab1=`echo $tab | sed 's/_final$//g'`
    file=`grep ${tab1}_sqoop  /tmp/dir_list | sort | tail -1`
    echo $tab $file
    hive -e "use tungsten_gp;alter table $tab set location 'hdfs://cerebro-namenode.snc1:8020${file}'"
done < /tmp/final_table_list

hive -e "use tungsten_gp; show tables '*_stg'" > /tmp/stg_table_list

while read tab; do
    hive -e "use tungsten_gp;drop table $tab"
done < /tmp/stg_table_list

