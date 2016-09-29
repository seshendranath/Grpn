td_schema=$1
td_table=$2
hive_table=${2}_full
inc_col=$3

current_Time=`date +%Y%m%d%H%M%S`

echo "python /home/svc_replicator_prod/Replicator/scripts/RunReplicator.py import --table=${td_schema}.${td_table} --hive_table=${td_schema}.${hive_table} -v | tee /tmp/${td_schema}_${td_table}_${current_Time}.log 2>&1"

python /home/svc_replicator_prod/Replicator/scripts/RunReplicator.py import --table=${td_schema}.${td_table} --hive_table=${td_schema}.${hive_table} -v | tee /tmp/${td_schema}_${td_table}_${current_Time}.log 2>&1

ret_code=${PIPESTATUS[0]}
echo "RETCODE:"$ret_code

if [[ "${ret_code}" -eq 0  ]]; then
      echo "Table Copy completed successfully"
else
      echo "Table Copy FAILED"
      exit 1
fi

from_clause="${td_schema}.${hive_table}"
drop_clause="drop table ${td_schema}.${hive_table};\n"
hdfs_rm="dfs -rm -r \/user\/grp_edw_rep_prod\/backup\/${td_schema}.${hive_table};\n"

sed -e "s/{from_clause}/${from_clause}/g;s/{inc_col}/${inc_col}/g;s/{td_schema}/${td_schema}/g;s/{td_table}/${td_table}/g;s/{drop_clause}/${drop_clause}/g;s/{hdfs_rm}/${hdfs_rm}/g" /home/svc_replicator_prod/Replicator/scripts/dpp.hql > ~/exportlogs/${td_schema}_${td_table}_$current_Time.hql

hive -f ~/exportlogs/${td_schema}_${td_table}_$current_Time.hql | tee /tmp/${td_schema}_${td_table}_${current_Time}_hql.log 2>&1

ret_code=${PIPESTATUS[0]}
echo "RETCODE:"$ret_code

if [[ "${ret_code}" -eq 0  ]]; then
      echo "Table Copy completed successfully"
else
      echo "Table Copy FAILED"
      exit 1
fi
