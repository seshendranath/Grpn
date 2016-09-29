#########################################PURGING DATA###################################
#Author: Ajay Guyyala

########################################################################################

user=svc_replicator_prod
base_loc=/user/grp_edw_rep_prod/backup
num_snapshots=3
files_to_purge=/home/${user}/Replicator/replicator_tables.txt

current_date=$(date +"%Y%m%d")
loc=/home/${user}/purge_logs/rid

mkdir -p ${loc}

for i in `cat ${files_to_purge}`
do

	schema_name=`echo $i | cut -d '.' -f1`
	table_name=`echo $i | cut -d '.' -f2`

	numFiles=`hadoop fs -ls ${base_loc}/${i}/ | wc -l`
	numFilestoDel=$(($numFiles-$num_snapshots));

if [[ $numFilestoDel -gt 1 ]]; then
	`hadoop fs -ls ${base_loc}/${i}/ | head -n $numFilestoDel | tail -n $(($numFilestoDel-1)) | awk '{print "hadoop fs -rm -r "$8}' > ${loc}/purge_dates_${schema_name}_${table_name}_$current_date.sh`

	sh ${loc}/purge_dates_${schema_name}_${table_name}_$current_date.sh >> ${loc}/purge.log 2>&1 &
fi

done

#`hadoop fs -rm -r /user/${user}/.Trash`
