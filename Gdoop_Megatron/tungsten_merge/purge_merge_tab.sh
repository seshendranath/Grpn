#!/bin/bash
export PATH=/usr/local/bin:/bin:/usr/sbin:/sbin:/usr/bin:/usr/local/sbin/:/usr/local/lib/hadoop/bin:/usr/local/lib/hive/bin

ds=${1:-`date "+%Y-%m-%d"`}
yesterday=${1:-`date --date="1 days ago" "+%Y-%m-%d"`}

hive -e "use groupon_orders; show tables '*_final'" > /tmp/final_table_list

hadoop dfs -ls -d /user/tungsten/groupon_orders/*_merge_* | grep -v "Found " | awk '{print $(NF)}' | sort  > /tmp/merge_dir_list

while read tab; do
    #loc=`hive -e "use groupon_orders;describe formatted $tab"  | grep "Location:"` 
    tab1=`echo $tab | sed 's/_final$//g'`
    dirs=`grep ${tab1}_merge_orders_2  /tmp/merge_dir_list| grep -v -e $ds -e $yesterday |tr '\n' ' '`
    if [ "$dirs" != "" ]; then
        hadoop dfs -rm -r -skipTrash $dirs
    fi
done < /tmp/final_table_list




