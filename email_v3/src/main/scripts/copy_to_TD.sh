#!/usr/bin/env bash

queue=public
tdDB=dev1_staging
tdUser=aguyyala
tdPWD=jan_2017
numMappers=60
srcTBL=svc_edw_dev_db.agg_email
tgtTBL=agg_email_new
batchSize=30000

export HIVE_HOME=/usr/local/lib/hive

export HADOOP_CLASSPATH="$HIVE_HOME/conf:$HIVE_HOME/lib/antlr-runtime-3.4.jar:$HIVE_HOME/lib/commons-dbcp-1.4.jar:$HIVE_HOME/lib/commons-pool-1.5.4.jar:$HIVE_HOME/lib/datanucleus-connectionpool-3.2.9.jar:$HIVE_HOME/lib/datanucleus-core-3.2.9.jar:$HIVE_HOME/lib/datanucleus-rdbms-3.2.9.jar:$HIVE_HOME/lib/hive-cli-1.2.1.jar:$HIVE_HOME/lib/hive-exec-1.2.1.jar:$HIVE_HOME/lib/hive-metastore-1.2.1.jar:$HIVE_HOME/lib/jdo-api-3.0.1.jar:$HIVE_HOME/lib/libfb303-0.9.2.jar:$HIVE_HOME/lib/libthrift-0.9.2.jar"

export TDCH_JAR=~/ajay/email/tdch/1.5/lib/teradata-connector-1.5.1.jar

export LIB_JARS="$HIVE_HOME/lib/hive-cli-1.2.1.jar,$HIVE_HOME/lib/hive-exec-1.2.1.jar,$HIVE_HOME/lib/hive-metastore-1.2.1.jar,$HIVE_HOME/lib/libfb303-0.9.2.jar,$HIVE_HOME/lib/libthrift-0.9.2.jar,$HIVE_HOME/lib/jdo-api-3.0.1.jar"

yarn jar $TDCH_JAR com.teradata.connector.common.tool.ConnectorExportTool -Dmapreduce.job.queuename=$queue -libjars $LIB_JARS -classname com.teradata.jdbc.TeraDriver -jobtype hive -url jdbc:teradata://tdwc/DATABASE=$tdDB  -username $tdUser -password $tdPWD -fileformat orc -nummappers $numMappers -sourcetable $srcTBL -targettable $tgtTBL -method batch.insert -queryband 'job=insert_test_new;' -batchsize $batchSize -forcestage true -errorlimit 30000
