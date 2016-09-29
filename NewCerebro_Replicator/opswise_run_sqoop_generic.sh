###############Variable Description################
###  script_host -> The Util Box where the Cerebro Copy script is stored
###  This script is a wrapper to invoke a sqoop task
###  Currently this is being used only for view user_groupondw.user_ent_txn_attr
###################################################


script_host="pit-share-hdputil1.snc1"

teradata_Database=`echo $1 | cut -d '.' -f1`
teradata_Table=`echo $1 | cut -d '.' -f2`
hive_Database=`echo $2 | cut -d '.' -f1`
hive_Table=`echo $2 | cut -d '.' -f2`
split_by_ID=$3
load=$4
inc_key=$5
inc_date=$6

current_Time=`date +%Y%m%d`


echo "ssh ${script_host} sh /home/etl_adhoc/Replicator/scripts/sqoopExtract/sqoop.sh ${teradata_Database}.${teradata_Table} ${hive_Database}.${hive_Table} ${split_by_ID} ${load} ${inc_key} ${inc_date} > /tmp/${teradata_Table}_${current_Time}.log 2>&1"
output=` ssh ${script_host} sh /home/etl_adhoc/Replicator/scripts/sqoopExtract/sqoop.sh ${teradata_Database}.${teradata_Table} ${hive_Database}.${hive_Table} ${split_by_ID} ${load} ${inc_key} ${inc_date} > /tmp/${teradata_Table}_${current_Time}.log 2>&1 `
ret_code=$?
echo "output = ${output}"
if [ "${ret_code}" -eq 0 ]; then
      echo "Script completed successfully"
else
      exit 1
fi
