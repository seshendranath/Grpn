#!/bin/bash

hdfsfolder="td_backup/groupon_production.redemption_sources/20150515"
hadoop jar /usr/local/lib/hadoop/lib/hadoop-lzo-current.jar com.hadoop.compression.lzo.DistributedLzoIndexer hdfs://cerebro-namenode.snc1:8020/${hdfsfolder}/lzo/
