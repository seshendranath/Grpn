#!/bin/bash

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
offset_days=$5
additional_params=$6

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


if [[ "${ret_code}" -eq 0 && "${TD_COUNT}" -le "${HIVE_COUNT}" ]]; then
      echo "Script completed successfully"
else
      echo "Script FAILED"
      exit 1
fi

if [[ ! -z $inc_key ]]; then
       if [[ $inc_val == *"-"* ]]; then
           inc_val=`date "--date=$inc_val-$i day" +%Y-%m-%d`
       else
           inc_val=`date "--date=$inc_val-$i day" +%Y%m%d`
       fi
fi

echo "###########################################################"
echo ""

done
