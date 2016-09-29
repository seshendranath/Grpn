###############Variable Description################
###  hdfs -> namenode of the hadoop cluster where the copy should happen
###  script_host -> The Util Box where the Cerebro Copy scropt is stored
###  schema -> Database of the table being copied
###  table -> Table being copied
###  inc_key -> partition key of the table, if the table is not partitioned, no argument should be passed
###  inc_val -> Date for which copy is suppoed to happen, for increment tables, this value could either be a key (integer yyyymmdd ) or a date (yyyy-mm-dd), for full refreshes, it assumes to be the current date



hdfs="hdfs://cerebro-namenode.snc1"
#script_host="pit-share-hdputil9.snc1"
schema=$1
table=$2
inc_key=$3
inc_val=$4
offset_days=$5
dsn=${cerebro_dsn}

ran=$(($RANDOM%3))
allwd_vrnc=1

if [ $ran == 0 ]; then
  script_host="pit-share-hdputil6.snc1"
else
  script_host="pit-share-hdputil9.snc1"
fi

# Start of Comment:- If the script is run without any arguments, the script will fail

if [[ -z "$schema" ]]; then
      echo "No Schema argument was passed, terminating"
      exit 1
else
    echo "Schema Name :- ${schema}"

# Checking if schema is megatron, based on that assertition logic will be mondified
        schema_meg=`echo $schema | tr '[a-z]' '[A-Z]'`
        if [ "$schema_meg" == "MEG_GRP_PROD" ]; then
           count_var_alwd=1
           echo "MEG_GRP_PROD table, percentage of count variance can be upto :" ${allwd_vrnc}
        else
           count_var_alwd=0
           echo "It is not MEG_GRP_PROD table, hence count variance is not allowed"
        fi

fi

if [ -z "$table" ]; then
    echo " No table argument was passed, terminating"
        exit 1
else
        echo "Table Name :- ${table}"

fi
# End of Comment

# An increment key should always have an increment value with it, else fail the process

if [ ! -z "$inc_key" ] && [ -z "$inc_val" ]; then
      echo "Provide an increment value for key: $inc_key"
      echo "NOTE: When an increment key is specified, a value should be provided"
      exit 1
fi

# End of Comment

#Start of Comment:- If inc_val is not passed as an argument, the script assumes that a full refresh is happening, and the inc_val for directory naming is defauled to the current date
if [ -z "$inc_val" ]; then
       echo "No inc_val and inc_key  was passed, Considering this table as a full refresh and defaulting to today's date "
       inc_val=`date +%Y%m%d`
       full_load=1

else
        date -d $inc_val
        rc=$?
        if [ $rc -ne 0 ]; then
            echo "Invalid date provided for increment value !!"
            echo "Inc value: $inc_val"
            exit 1
        fi
    echo " Picking the inc_val as ${inc_val} "
        full_load=0

fi

#End of Comment

# The offset defines the no. of of days data that should be loaded into Cerebro. For Eg if 7, then we copy over date, date-1, ... date-6

if [ -z "$offset_days" ]; then
      echo "No offset_days specified, loading just for a single date"
      offset_days=1
elif [ -z "$inc_val" ] && [ -z "$inc_key" ]; then
      echo "A full refresh cannot have an offset value!!"
      exit 1
else
      echo "No. of offset_days specified = $offset_days"
fi

org_inc_val=$inc_val

# Check if the inc_value provided is a date_key or a date
if echo $inc_val | egrep -q '^[0-9]+$'; then
     echo "We don't have hiphens indate"
     date_key="Y"
     length=`expr length "$inc_val"`
     #Inc_val should be in format yyyyddmm
     if [ "$length" -ne 8 ]; then
         echo "Invalid/Incorrect increment value for creating partition, inc_val = $inc_val"
         exit 1
     fi
else
     echo "There are hiphens"
     date_key="N"
     length=`expr length "$inc_val"`
     #Inc_val should be in format yyyy-dd-mm
      if [ "$length" -ne 10 ]; then
          echo "Invalid/Incorrect increment value for creating partition, inc_val = $inc_val"
          exit 1
      fi
fi

for i in `seq 1 $offset_days`
do
      echo "###########################################################"
      echo "Processing for date: $inc_val"
      echo "###########################################################"

# Start of Comment:- Check if the file exists for the date of copy that we are trying to do, and a warning message would be printed to alert, but the script will not fail
inc_val_stripped=`echo "${inc_val//-/}"`
echo "hadoop fs -ls ${hdfs}/td_backup/${schema}.${table}/${inc_val_stripped}"
hadoop fs -ls ${hdfs}/td_backup/${schema}.${table}/${inc_val_stripped}
ret_code=$?

if [ "$ret_code" -eq 0 ]; then
    echo " File Already Exist for the date ${inc_val} , Did you mean to re-load it?"
    echo " Assuming that a backfill is happening, and continuing, if this is not the intention, then contact swason@"
    #job_status="failed"
fi
#else
#End of Comment

start_dt=`date "+20%y-%m-%d %T"`

#Start of Comment:- Starting the import of the table, different zombie script will run depending on the format(yyyymmdd or yyyy-mm-dd) of the increment key being passed
echo "Starting Import of ${schema}.${table}"
if [ $full_load -eq 0 ]; then
    echo " Picking the increment value as ${inc_val} "

        echo "ssh ${script_host} PYTHONPATH=$PYTHONPATH:/home/etl_adhoc/Replicator/ZombieRunner  PATH=/usr/local/bin/unixodbc:/opt/teradata/client/Current/tbuild/bin:/usr/local/bin:/usr/local/sbin:/usr/bin:/usr/sbin:/bin:/sbin:/home/etl_adhoc/bin zombie_runner run /home/etl_adhoc/Replicator/scripts/backup2cerebro/ --context=schema:${schema},base_table:${table},inc_key:\x22${inc_key}\x22,inc_date:${inc_val}"
            output=` ssh ${script_host} PYTHONPATH=$PYTHONPATH:/home/etl_adhoc/Replicator/ZombieRunner  PATH=/usr/local/bin/unixodbc:/opt/teradata/client/Current/tbuild/bin:/usr/local/bin:/usr/local/sbin:/usr/bin:/usr/sbin:/bin:/sbin:/home/etl_adhoc/bin zombie_runner run /home/etl_adhoc/Replicator/scripts/backup2cerebro/ --context=schema:${schema},base_table:${table},inc_key:\"${inc_key}\",inc_date:${inc_val}`

else
       echo "No inc_val and inc_key  was passed, Considering this table as a full refresh and defauling to today's date "
       inc_val=`date +%Y%m%d`
        echo "ssh ${script_host} PYTHONPATH=$PYTHONPATH:/home/etl_adhoc/Replicator/ZombieRunner  PATH=/usr/local/bin/unixodbc:/opt/teradata/client/Current/tbuild/bin:/usr/local/bin:/usr/local/sbin:/usr/bin:/usr/sbin:/bin:/sbin:/home/etl_adhoc/bin zombie_runner run /home/etl_adhoc/Replicator/scripts/backup2cerebro/ --context=schema:${schema},base_table:${table}"

        output=` ssh ${script_host} PYTHONPATH=$PYTHONPATH:/home/etl_adhoc/Replicator/ZombieRunner  PATH=/usr/local/bin/unixodbc:/opt/teradata/client/Current/tbuild/bin:/usr/local/bin:/usr/local/sbin:/usr/bin:/usr/sbin:/bin:/sbin:/home/etl_adhoc/bin zombie_runner run /home/etl_adhoc/Replicator/scripts/backup2cerebro/ --context=schema:${schema},base_table:${table} `

fi

       ret_code=$?
       echo "$output"
       echo "$output" |grep -v "ERROR serde.TDFastExportInputFormat: found end"|grep -v "ERROR [root:131] - File not found"|grep -v "Error walking path:"| grep -v "ERROR for block" | grep -v "Error Recovery" | grep -i "error "
       if [[ $? -eq 0 ]] ; then
           echo " Found Error!!"
           job_status="failed"
       fi
       if [ "$ret_code" -ne 0 ]; then
             job_status="failed"
       fi

#End of the Comment

end_dt=`date "+20%y-%m-%d %T"`

# Parsing the logs to extract the source and destination row count
pattern_to_search="Manifest"
new_output=` echo ${output/*$pattern_to_search/$pattern_to_search} `
 echo "$new_output" | grep ")-"
       if [[ $? -eq 0 ]] ; then
            new_pattern_to_search=")-"
       else
        new_pattern_to_search="|"
       fi

final_output=`echo ${new_output/$new_pattern_to_search*/$new_pattern_to_search} `
td_count=` echo $output |  egrep -o 'Manifest\([^\d]*\) ='|grep -o "[0-9]\+"`
hive_count=` echo $output | egrep -o 'LZO\([^\d]*\) '|grep -o  "[0-9]\+"`
hive_count_trimmed=` echo $hive_count|sed 's/[a-z]//g'|sed 's/[A-Z]//g'|sed 's/://g' `
len=`expr length "$td_count"`
echo " Length=${len}"
hive_count_trimmed=`echo "${hive_count_trimmed}"|cut -c1-"${len}"`
echo " Teradata Count : ${td_count}"
echo " Hive Count : ${hive_count_trimmed}"

#checking if schema is megatron and allowing count variance of upto 1 %

if [ ${count_var_alwd} == 0 ]; then
 if [ "${td_count}" -eq "${hive_count_trimmed}" ]; then
    echo "Counts have matched, Success!!"
        if [ "${job_status}" != "failed" ]; then
              job_status="complete"
        fi
 else
    echo " Counts have not matched, exiting, and the script will fail "
        job_status="failed"
 fi

else
 var_cnt_mns=$(expr ${td_count} - ${hive_count_trimmed})
 var_prcntg=$(expr 100 \* ${var_cnt_mns} / ${td_count} )
 if [ ${var_prcntg} -lt ${allwd_vrnc} ]; then
        echo "Counts is within variance range, count check is Success!!"
        if [ "${job_status}" != "failed" ]; then
              job_status="complete"
        fi
 else
        echo " Counts variance is higher than the allowed variance of %: " ${allwd_vrnc}
        job_status="failed"
 fi

fi



#Check if the counts retreived is not junk value
if echo $td_count | egrep -q '^[0-9]+$'; then
       echo ""
else
       echo "Some junk value retreived for count, which is incorrect"
       td_count=0
       hive_count_trimmed=0
       job_status="failed"
fi



#Update the cerebro metadata with the relevant info about the copy
        echo "/usr/local/bin/python /var/groupon/etl/deploy/etl/cerebro/connect_metadata.py --dsn ${dsn} --table ${table} --start "${start_dt}" --end "${end_dt}" --td_count ${td_count} --hv_count ${hive_count_trimmed} --job_status ${job_status} --inc_val ${org_inc_val} --schema ${schema}"

        output=` /usr/local/bin/python /var/groupon/etl/deploy/etl/cerebro/connect_metadata.py --dsn ${dsn} --table ${table} --start "${start_dt}" --end "${end_dt}" --td_count ${td_count} --hv_count ${hive_count_trimmed} --job_status ${job_status} --inc_val ${org_inc_val} --schema ${schema}`
        rc=$?

        echo $output|sed 's/\*\*\*/\n/g'
        if [ $rc -ne 0 ]; then
             echo ""
             echo "Something went wrong while updating metadata info!!"
             #exit 1
        fi

if [ "${job_status}" == "failed" ]; then
       echo "Processing failed for date : $inc_val"
       exit 1
fi

if [ $date_key == "Y" ]; then
       inc_val=`date "--date=$org_inc_val-$i day" +%Y%m%d`
else
       inc_val=`date "--date=$org_inc_val-$i day" +%Y-%m-%d`
fi

echo "###########################################################"
echo ""

done
