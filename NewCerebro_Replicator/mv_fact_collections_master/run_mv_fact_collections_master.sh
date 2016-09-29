###############Variable Description################
###  script_host -> The Util Box where the Cerebro Copy script is stored
###  This script is a wrapper to invoke cerebro copy for dw.mv_fact_collections_master
###  Currently this is being used only for dw.mv_fact_collections_master
###################################################


script_host="svc_replicator_prod@cerebro-replicator1.snc1"
user="svc_replicator_prod"
db="dw"
table="mv_fact_collections_master"
done_dir="hdfs://cerebro-namenode/user/grp_edw_rep_prod/backup/done"

run_dt=`date "+%Y%m%d"`

echo "ssh ${script_host} hive -f /home/${user}/Replicator/${table}/${table}.hql > /tmp/${table}_${run_dt}.log"
output=` ssh ${script_host} hive -f /home/${user}/Replicator/${table}/${table}.hql > /tmp/${table}_${run_dt}.log `
ret_code=$?
echo "output = ${output}"
if [ "${ret_code}" -eq 0 ]; then
      echo "Script completed successfully"
else
      exit 1
fi

ssh ${script_host} hadoop fs -mkdir -p ${done_dir}/${db}.${table}/${run_dt}/manifest/
ret_code=$?
if [ "${ret_code}" -eq 0 ]; then
       echo "Dir for done file created successfully"
else
      echo "Error in creating Dir for done file"
      exit 1
fi

ssh ${script_host} hadoop fs -touchz ${done_dir}/${db}.${table}/${run_dt}/manifest/${db}.${table}.D.manifest.done
ret_code=$?
if [ "${ret_code}" -eq 0 ]; then
       echo "Done file created successfully"
else
      echo "Error in creating done file"
      exit 1
fi
