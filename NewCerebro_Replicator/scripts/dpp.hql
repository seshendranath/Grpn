SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions.pernode=10000;
SET hive.exec.max.dynamic.partitions=10000;
SET hive.exec.max.created.files=300000;

dfs -rm -r /user/grp_edw_rep_prod/backup/{td_schema}.{td_table}/*;

alter table {td_schema}.{td_table} set location 'hdfs://cerebro-namenode/user/grp_edw_rep_prod/backup/{td_schema}.{td_table}';

INSERT OVERWRITE TABLE {td_schema}.{td_table} partition (ds)
SELECT
*,
regexp_replace({inc_col},'-','') as ds
from {from_clause}
DISTRIBUTE BY {inc_col};

alter table {td_schema}.{td_table} set location 'hdfs://cerebro-namenode/tmp/replicator';

{drop_clause}

{hdfs_rm}

