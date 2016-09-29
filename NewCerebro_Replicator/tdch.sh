#!/bin/bash

db=$1
table=$2
dt=`date +%Y%m%d`
tddb=tdwc
user=cerebroreplicator
password=edw_infra_cerebro7

td_Count=$(bteq << EOF 2>&1 | grep '^>' | sed -e "s/^>//"
                .LOGON ${tddb}/${user},${password}
                select '>'|| count(*) from ${db}.${table};
                .LOGOFF;
                .QUIT;
                .EXIT;
                EOF)

export HIVE_HOME=/usr/local/lib/hive

export HADOOP_CLASSPATH="$HIVE_HOME/conf:$HIVE_HOME/lib/antlr-runtime-3.4.jar:$HIVE_HOME/lib/commons-dbcp-1.4.jar:$HIVE_HOME/lib/commons-pool-1.5.4.jar:$HIVE_HOME/lib/datanucleus-connectionpool-2.0.3.jar:$HIVE_HOME/lib/datanucleus-core-2.0.3-ZD5977-CDH5293.jar:$HIVE_HOME/lib/datanucleus-rdbms-2.0.3.jar:$HIVE_HOME/lib/hive-cli-0.10.0-cdh4.2.1.jar:$HIVE_HOME/lib/hive-exec-0.10.0-cdh4.2.1.jar:$HIVE_HOME/lib/hive-metastore-0.10.0-cdh4.2.1.jar:$HIVE_HOME/lib/jdo2-api-2.3-ec.jar:$HIVE_HOME/lib/libfb303-0.9.0.jar:$HIVE_HOME/lib/libthrift-0.9.0.jar"

#export TDCH_JAR=/usr/lib/tdch/1.4/lib/teradata-connector-1.4.2.jar
export TDCH_JAR=/home/etl_adhoc/ajay/tdch/1.4/lib/teradata-connector-1.4.2.jar

export LIB_JARS="$HIVE_HOME/lib/antlr-runtime-3.4.jar,$HIVE_HOME/lib/commons-dbcp-1.4.jar,$HIVE_HOME/lib/commons-pool-1.5.4.jar,$HIVE_HOME/lib/datanucleus-connectionpool-2.0.3.jar,$HIVE_HOME/lib/datanucleus-core-2.0.3-ZD5977-CDH5293.jar,$HIVE_HOME/lib/datanucleus-rdbms-2.0.3.jar,$HIVE_HOME/lib/hive-cli-0.10.0-cdh4.2.1.jar,$HIVE_HOME/lib/hive-exec-0.10.0-cdh4.2.1.jar,$HIVE_HOME/lib/hive-metastore-0.10.0-cdh4.2.1.jar,$HIVE_HOME/lib/jdo2-api-2.3-ec.jar,$HIVE_HOME/lib/libfb303-0.9.0.jar,$HIVE_HOME/lib/libthrift-0.9.0.jar"

out=` hadoop fs -rm -r /td_backup/${db}.${table}/${dt}/lzo `

echo $out

echo "hadoop jar $TDCH_JAR com.teradata.connector.common.tool.ConnectorImportTool -Dmapred.compress.map.output=true -Dmapred.output.compress=true -Dmapred.output.compression.codec=com.hadoop.compression.lzo.LzopCodec -Dmapred.map.output.compression.codec=com.hadoop.compression.lzo.LzopCodec -Dmapred.map.output.compress.codec=com.hadoop.compression.lzo.LzopCodec -libjars $LIB_JARS -classname com.teradata.jdbc.TeraDriver -url jdbc:teradata://tdwc/DATABASE=${db}  -username TD2CEREBROMOVER -password edw_infra_cerebro7 -jobtype hdfs -fileformat textfile -method split.by.amp -nummappers 64 -targetpaths /td_backup/${db}.${table}/${dt}/lzo -sourcetable ${table} -separator '\u0001'"

output=`hadoop jar $TDCH_JAR com.teradata.connector.common.tool.ConnectorImportTool -Dmapred.compress.map.output=true -Dmapred.output.compress=true -Dmapred.output.compression.codec=com.hadoop.compression.lzo.LzopCodec -Dmapred.map.output.compression.codec=com.hadoop.compression.lzo.LzopCodec -Dmapred.map.output.compress.codec=com.hadoop.compression.lzo.LzopCodec -libjars $LIB_JARS -classname com.teradata.jdbc.TeraDriver -url jdbc:teradata://tdwc/DATABASE=${db}  -username TD2CEREBROMOVER -password edw_infra_cerebro7 -jobtype hdfs -fileformat textfile -method split.by.amp -nummappers 64 -targetpaths /td_backup/${db}.${table}/${dt}/lzo -sourcetable ${table} -separator '\u0001' > ${table}_${dt}.log 2>&1`

if [[ $? -ne 0 ]] ; then
   echo " Found Error!! "
   echo "${table} FAILED loading to Cerebro" | mailx -s "${db} FAILED loading to Cerebro" -r aguyyala@groupon.com aguyyala@groupon.com
fi

indexer=` hadoop jar /usr/local/lib/hadoop/lib/hadoop-lzo-current.jar com.hadoop.compression.lzo.DistributedLzoIndexer hdfs://cerebro-namenode/td_backup/${db}.${table}/${dt}/lzo/ `

echo $indexer

count=` cat ${table}_${dt}.log | grep -i "Map output records=" | awk -F'=' '{print $2}' `

echo $output

echo $table,$count >> fact_count.txt

ho=$(hive -e "use ${db}; alter table ${table} set location 'hdfs://cerebro-namenode/td_backup/${db}.${table}/${dt}/lzo';")

if [[ $? -ne 0 ]] ; then
   echo " Found Error!! "
   echo "${table} FAILED loading to Cerebro" | mailx -s "${table} FAILED loading to Cerebro" -r aguyyala@groupon.com aguyyala@groupon.com
fi

echo $ho

echo "Teradata Count: " $td_Count
echo "Hive Count:" $count

if [[ "$td_Count" -eq "$count" ]]; then
status=CORRECT
echo "Counts have matched"
done=`hadoop fs -touchz /td_backup_done/${db}.${table}/${dt}/manifest/${db}.${table}.D.manifest.done`
else
status=MISMATCH

echo "${table} status: ${status} Teradata count: ${td_Count} Hive_count: ${count} in Cerebro" | mailx -s "${table} status: ${status} Teradata count: ${td_Count} Hive_count: ${count} in Cerebro" -r edw-infra@groupon.com aguyyala@groupon.com

echo " Found Error!! or Counts have not matched Script will FAIL"

exit 1
fi
