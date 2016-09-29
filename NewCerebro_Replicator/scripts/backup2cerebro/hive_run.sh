#!/bin/bash

table="prod_groupondw.incentive_qualification"
partition=20150515
namenode="hdfs://cerebro-namenode.snc1:8020"

hadoop fs -rm -r ${namenode}/td_backup/${table}/${partition}
hadoop fs -mkdir ${namenode}/td_backup/${table}/${partition}

hadoop fs -mv ${namenode}/tmp/${table}/${partition}/* ${namenode}/td_backup/${table}/${partition}

./create_hive_table.py --backup_dir=hdfs://cerebro-namenode.snc1:8020/td_backup  \
		       --done_dir=hdfs://cerebro-namenode.snc1:8020/td_backup_done \
			--table_name=${table} \
			--part=${partition} -vv \
			--hive_table=${table}

hadoop jar /usr/local/lib/hadoop/lib/hadoop-lzo-current.jar com.hadoop.compression.lzo.DistributedLzoIndexer hdfs://cerebro-namenode.snc1:8020/td_backup/${table}/${partition}/lzo/


