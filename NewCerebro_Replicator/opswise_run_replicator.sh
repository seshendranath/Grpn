#!/bin/bash
set -x
###############Variable Description################
###  script_host -> The Util Box where the Cerebro Copy script is stored
###  This script is a wrapper to invoke a Replicator task
###################################################

set -o pipefail

user="svc_replicator_prod"
script_host="cerebro-replicator1.snc1"
loc="/home/svc_replicator_prod/Replicator/scripts/RunReplicator.py"

teradata_Database=$1
teradata_Table=$2
inc_key=$3
inc_val=$4
orig_inc_val=$4
offset_days=$5
additional_params=$6
allwd_vrnc=1

# Checking if schema is megatron, based on that assertition logic will be mondified

TD_DB_MEG=`echo ${teradata_Database} | tr '[a-z]' '[A-Z]'`
if [ "${TD_DB_MEG}" == "MEG_GRP_PROD" ]; then
   count_var_alwd=1
   echo "MEG_GRP_PROD table, so percentage of count variance can be upto:" ${allwd_vrnc}
else
   count_var_alwd=0
   echo "It is not MEG_GRP_PROD table, hence count variance is not allowed"
fi


for i in `seq 1 $offset_days`
do
      echo "###########################################################"
      echo "Processing for date: $inc_val"
      echo "###########################################################"

if [[ ! -z $inc_key ]];then
  part_str="--inc_key=${inc_key} --inc_val=${inc_val}"
fi


current_Time=`date +%Y%m%d%H%M%S`

echo "ssh ${user}@${script_host} python ${loc} import --table=${teradata_Database}.${teradata_Table} $part_str $additional_params -v | tee /tmp/${teradata_Database}_${teradata_Table}_${current_Time}.log 2>&1"

ssh ${user}@${script_host} python ${loc} import --table=${teradata_Database}.${teradata_Table} $part_str $additional_params -v | tee /tmp/${teradata_Database}_${teradata_Table}_${current_Time}.log 2>&1

ret_code=${PIPESTATUS[0]}
echo "RETCODE:"$ret_code

TD_COUNT=`cat /tmp/${teradata_Database}_${teradata_Table}_${current_Time}.log | grep -P 'TD\(\d+\)=Hive\(\d+\)' -o | awk -F'=' '{print $1}' | grep -o '[0-9]*'`
HIVE_COUNT=`cat /tmp/${teradata_Database}_${teradata_Table}_${current_Time}.log | grep -P 'TD\(\d+\)=Hive\(\d+\)' -o | awk -F'=' '{print $2}' | grep -o '[0-9]*'`

echo "TD_COUNT = ${TD_COUNT}"
echo "HIVE_COUNT = ${HIVE_COUNT}"

if [ ${count_var_alwd} == 0 ]; then
  if [[ "${ret_code}" -eq 0 && "${TD_COUNT}" -le "${HIVE_COUNT}" ]]; then
      echo "Script completed successfully"
  else
      echo "Script FAILED"
      exit 1
  fi
else
 var_cnt_mns=$(expr ${TD_COUNT} - ${HIVE_COUNT})
 var_prcntg=$(expr 100 \* ${var_cnt_mns} / ${TD_COUNT} )
 if [[ "${ret_code}" -eq 0 && ${var_prcntg} -lt ${allwd_vrnc} ]]; then
      echo "Count check is Success!!, Counts is within variance % :" ${allwd_vrnc}
 else
      echo "Count check failed as the percentage of count variance is greater than " ${allwd_vrnc}
      exit 1
 fi
fi


if [[ ! -z $inc_key ]]; then
       if [[ $inc_val == *"-"* ]]; then
           inc_val=`date "--date=$orig_inc_val-$i day" +%Y-%m-%d`
       else
           inc_val=`date "--date=$orig_inc_val-$i day" +%Y%m%d`
       fi
fi

echo "###########################################################"
echo ""

done
