#!/usr/bin/env bash

export HIVE_HOME=/usr/local/lib/hive

export HADOOP_CLASSPATH="$HIVE_HOME/conf:$HIVE_HOME/lib/antlr-runtime-3.4.jar:$HIVE_HOME/lib/commons-dbcp-1.4.jar:$HIVE_HOME/lib/commons-pool-1.5.4.jar:$HIVE_HOME/lib/datanucleus-connectionpool-3.2.9.jar:$HIVE_HOME/lib/datanucleus-core-3.2.9.jar:$HIVE_HOME/lib/datanucleus-rdbms-3.2.9.jar:$HIVE_HOME/lib/hive-cli-1.2.1.jar:$HIVE_HOME/lib/hive-exec-1.2.1.jar:$HIVE_HOME/lib/hive-metastore-1.2.1.jar:$HIVE_HOME/lib/jdo-api-3.0.1.jar:$HIVE_HOME/lib/libfb303-0.9.2.jar:$HIVE_HOME/lib/libthrift-0.9.2.jar"

export TDCH_JAR=tdch/1.5/lib/teradata-connector-1.5.1.jar

export LIB_JARS="$HIVE_HOME/lib/hive-cli-1.2.1.jar,$HIVE_HOME/lib/hive-exec-1.2.1.jar,$HIVE_HOME/lib/hive-metastore-1.2.1.jar,$HIVE_HOME/lib/libfb303-0.9.2.jar,$HIVE_HOME/lib/libthrift-0.9.2.jar,$HIVE_HOME/lib/jdo-api-3.0.1.jar"

yarn jar $TDCH_JAR com.teradata.connector.common.tool.ConnectorExportTool -Dmapreduce.job.queuename=edw_traffic_core -libjars $LIB_JARS -classname com.teradata.jdbc.TeraDriver -jobtype hive -url jdbc:teradata://tdwc/DATABASE=sandbox  -username b_tdhadoopmover -password sy6gasq4ic -fileformat orc -nummappers 61 -sourcetable svc_edw_dev_db.fact_email_non_us -targettable fact_email -method batch.insert -queryband 'job=insert_test2;' -batchsize 30000 

