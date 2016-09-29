#!/bin/bash
#set -eo pipefail

#set -x
#  Script to automatically load data from Teradata to hive using sqoop import
#  This script required Teradata database.table, hive database.table, split_by_id as parameters

#Checking whether the parameters are passed correctly
if [[ $# -lt 4 ]] || [[ "$4" != "full" &&  "$4" != "inc" ]] || [[ "$4" == "inc" &&  -z $5 ]];
then
        echo "Kindly specify right parameters, 1. Teradata Database.Table, 2. Hive Database.Table, 3. split_by_id 4. load(full or inc) 5. inc_key 6.inc_date"
        exit 1
fi

#Assigning values to variables
mail_Content_Dir=$(dirname $0)
teradata_Database=`echo $1 | cut -d '.' -f1`
teradata_Table=`echo $1 | cut -d '.' -f2`
hive_Database=`echo $2 | cut -d '.' -f1`
hive_Table=`echo $2 | cut -d '.' -f2`
current_Time=`date +%Y%m%d`
current_Time_ymd=`date +%Y-%m-%d`
current_Time_ymd_p=`date "--date=${current_Time}-1 day" +%Y-%m-%d`
split_by_ID=$3
load=$4
inc_key=$5
inc_date=$6
system=cerebro

home=/home/svc_replicator_prod
prefix=/user/grp_edw_rep_prod/backup
replicator_path=${home}/Replicator
zr_context_path=${home}/.zrc2

db=`grep "source_user: " ${zr_context_path} | awk '{print $2}' | awk -F'/' '{print $1}' | tr -d ' '`
user=`grep "source_user: " ${zr_context_path} | awk -F'/' '{print $2}' | tr -d ' '`
password=`grep "source_pass: " ${zr_context_path} | awk '{print $2}' | tr -d ' '`

n_maps=6

#Creating respective directories
echo "Creating Directories in ${mail_Content_Dir}"
mkdir -p ${mail_Content_Dir}/logs
mkdir -p ${mail_Content_Dir}/mail
mkdir -p ${mail_Content_Dir}/java

echo "teradata DB name is " $teradata_Database
echo "teradata table name is " $teradata_Table
echo "hive DB name is " $hive_Database
echo "hive Table name is " $hive_Table
echo "current date is " $current_Time
echo "split by id is " $split_by_ID

# Setting up Email Notifications
rm -f ${mail_Content_Dir}/mail/mailcontent_${hive_Table}.html
echo "From:aguyyala@groupon.com
To:aguyyala@groupon.com
Subject: Information :: Table ${hive_Table} ${system} Load Status
MIME-Version: 1.0
Content-Type: text/html
Content-Disposition: inline
<!DOCTYPE html>
<html>
<body>
<p>Hi,<br><br>This is to inform the ${hive_Table} load status in ${system}</p>" >> ${mail_Content_Dir}/mail/mailcontent_${hive_Table}.html

# Checking the teradata count before the sqoop process

if [ "${load}" == "inc" ]; then
	if [[ ! -z $inc_date ]]; then
		inc_dt=$inc_date
	else
		inc_dt=$current_Time_ymd_p
	fi

if [[ `echo ${inc_dt} | sed 's/[0-9]*//g'` != "--" ]]; then
	inc_dt=`date "--date=${inc_dt}" +%Y-%m-%d`
fi

where_clause="where $inc_key='$inc_dt'"
hive_where_clause="where ds='${inc_dt//-/}'"
else
inc_dt=$current_Time
fi

td_Count=$(
bteq << EOF 2>&1 | grep '^>' | sed -e "s/^>//"
.LOGON ${db}/${user},${password}
select '>'|| count(*) from ${teradata_Database}.${teradata_Table} $where_clause;
.LOGOFF;
.QUIT;
.EXIT;
EOF
)

echo "<p>Teradata count for the table ${teradata_Table} ${inc_dt} is ${td_Count}</p>" >> ${mail_Content_Dir}/mail/mailcontent_${hive_Table}.html


if [[ ! -z $hive_Database ]] && [[ ! -z $hive_Table ]] && [[ ! -z $inc_dt ]]; then
hadoop fs -test -d ${prefix}/${hive_Database}.${hive_Table}/${inc_dt//-/}/
TestDir=$?
	if [ $TestDir  -eq  0 ]; then
                hadoop fs -rm -r ${prefix}/${hive_Database}.${hive_Table}/${inc_dt//-/}/
		echo "Directory Deleted"
 	else
                echo "Directory does not Exist"
	fi
fi

if [ "${load}" == "full" ]; then

echo "sqoop import > ${mail_Content_Dir}/logs/${teradata_Table}_${current_Time} 2>&1"

echo "sqoop import -Dmapred.job.queue.name=public --connect jdbc:teradata://${db}/DATABASE=${teradata_Database} --connection-manager org.apache.sqoop.teradata.TeradataConnManager --username ${user} --password ${password} --table ${teradata_Table} -m ${n_maps} --target-dir ${prefix}/${hive_Database}.${hive_Table}/${inc_dt}/ --split-by ${split_by_ID}  --optionally-enclosed-by '\' --fields-terminated-by '\001' --escaped-by \\ --hive-drop-import-delims 2>&1 | tee ${mail_Content_Dir}/logs/${teradata_Table}_${current_Time}"

sqoop import -Dmapred.job.queue.name=public --connect jdbc:teradata://${db}/DATABASE=${teradata_Database} --connection-manager org.apache.sqoop.teradata.TeradataConnManager --username ${user} --password ${password} --table ${teradata_Table} -m ${n_maps} --target-dir ${prefix}/${hive_Database}.${hive_Table}/${inc_dt}/ --split-by ${split_by_ID}  --optionally-enclosed-by '\"' --fields-terminated-by '\001' --escaped-by \\ --hive-drop-import-delims 2>&1 | tee ${mail_Content_Dir}/logs/${teradata_Table}_${current_Time}

if [ $? -ne 0 ]; then
        echo "<p>Sqoop Import from teradata got failed</p>" >> ${mail_Content_Dir}/mail/mailcontent_${hive_Table}.html
        exit 1
else
        echo "<p>Sqoop Import from teradata is successful</p>" >> ${mail_Content_Dir}/mail/mailcontent_${hive_Table}.html
fi

drop_table="drop table if exists ${hive_Table};"
alter_table="set"

else

sqoop import  -Dmapred.job.queue.name=public --connect jdbc:teradata://${db}/DATABASE=$teradata_Database --connection-manager org.apache.sqoop.teradata.TeradataConnManager --username ${user} --password ${password} --table ${teradata_Table} -m ${n_maps} --where "${inc_key}='${inc_dt}'" --target-dir ${prefix}/${hive_Database}.${hive_Table}/${inc_dt//-/}/ --split-by ${split_by_ID} --optionally-enclosed-by '\"' --fields-terminated-by '\001' --escaped-by \\ --append 2>&1 | tee ${mail_Content_Dir}/logs/${teradata_Table}_${inc_dt//-/}

if [ $? -ne 0 ]; then
        echo "<p>Sqoop Import from teradata got failed</p>" >> ${mail_Content_Dir}/mail/mailcontent_${hive_Table}.html
        exit 1
else
        echo "<p>Sqoop Import from teradata is successful</p>" >> ${mail_Content_Dir}/mail/mailcontent_${hive_Table}.html
fi

alter_table="add if not exists partition(ds=${inc_dt//-/})"
partition="--hive-partition-key ds"

fi

hive -e  " CREATE DATABASE IF NOT EXISTS ${hive_Database};"

sqoop create-hive-table --connect jdbc:teradata://${db}/DATABASE=${teradata_Database} --connection-manager org.apache.sqoop.teradata.TeradataConnManager --username ${user} --password ${password} --table ${teradata_Table}  --hive-table ${hive_Database}.${hive_Table}_schema_only ${partition} --optionally-enclosed-by '\"' --fields-terminated-by '\001' --escaped-by \\

hive -e  " use ${hive_Database}; ${drop_table} create external table if not exists ${hive_Table} like ${hive_Table}_schema_only; "
hive -e  " use ${hive_Database}; drop table ${hive_Table}_schema_only;"

hive -e "use ${hive_Database}; alter table ${hive_Table} ${alter_table} location 'hdfs://${system}-namenode${prefix}/${hive_Database}.${hive_Table}/${inc_dt//-/}/';"

if [ $? -ne 0 ]; then
        echo "<p>CRITICAL:Hive table is not pointed to recent hdfs imported data</p>" >> ${mail_Content_Dir}/mail/mailcontent_${hive_Table}.html
else
        echo "<p>Hive table is pointed to recently imported data</p>" >> ${mail_Content_Dir}/mail/mailcontent_${hive_Table}.html
fi

#mv ${mail_Content_Dir}/${teradata_Table}.java java/

# Checking the hive count
hive_Count=`hive -e "select count(*) from ${hive_Database}.${hive_Table} $hive_where_clause"`
echo "<p>Hive count for the table ${hive_Table} ${inc_dt} is ${hive_Count}</p>" >> ${mail_Content_Dir}/mail/mailcontent_${hive_Table}.html

echo " Teradata Count : ${td_Count}"
echo " Hive Count : ${hive_Count}"

if [[ "$td_Count" -eq "$hive_Count" ]]; then
status=CORRECT
echo "Counts have matched"

# Done File creation

hadoop fs -mkdir -p ${prefix}/done/${hive_Database}.${hive_Table}/${inc_dt//-/}/manifest/
hadoop fs -touchz ${prefix}/done/${hive_Database}.${hive_Table}/${inc_dt//-/}/manifest/${hive_Database}.${hive_Table}.D.manifest.done

echo "hadoop fs -touchz ${prefix}/done/${hive_Database}.${hive_Table}/${inc_dt}/manifest/${hive_Database}.${hive_Table}.D.manifest.done"
if [ $? -ne 0 ]; then
        echo "<p>Done File creation failed</p>" >> ${mail_Content_Dir}/mail/mailcontent_${hive_Table}.html
else
        echo "<p>Done File Created Successfully</p>" >> ${mail_Content_Dir}/mail/mailcontent_${hive_Table}.html
fi

echo "</table><p>This is a system generated mail. Please do not reply to this mail.<br><br>Thanks,<br>EDW Infra Team.<br></p></body></html>" >> ${mail_Content_Dir}/mail/mailcontent_${hive_Table}.html

cat ${mail_Content_Dir}/mail/mailcontent_${hive_Table}.html|/usr/lib/sendmail -t

else
status=MISMATCH

echo "${hive_Table} ${inc_dt} status: ${status} Teradata count: ${td_Count} Hive_count: ${hive_Count} in Cerebro" | mailx -s "${hive_Table} ${inc_dt} status: ${status} Teradata count: ${td_Count} Hive_count: ${hive_Count} in Cerebro" -r edw-infra@groupon.com aguyyala@groupon.com

echo " Found Error!! or Counts have not matched Script will FAIL"

exit 1
fi
