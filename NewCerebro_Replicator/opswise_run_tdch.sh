###############Variable Description################
###  script_host -> The Util Box where the Cerebro Copy script is stored
###  This script is a wrapper to invoke a sqoop task
###  Currently this is being used only for view user_groupondw.user_ent_txn_attr
###################################################


script_host="pit-share-hdputil9.snc1"

teradata_Database=$1
teradata_Table=$2

current_Time=`date +%Y%m%d`

echo "ssh ${script_host} sh /home/etl_adhoc/Replicator/run.sh ${teradata_Database} ${teradata_Table}  > /tmp/${teradata_Table}_${current_Time}.log 2>&1"
output=` ssh ${script_host} sh /home/etl_adhoc/Replicator/run.sh ${teradata_Database} ${teradata_Table}  > /tmp/${teradata_Table}_${current_Time}.log 2>&1  `
ret_code=$?
echo "output = ${output}"
if [ "${ret_code}" -eq 0 ]; then
      echo "Script completed successfully"
else
      exit 1
fi
