for i in `cat t.txt`
do
	hdfs dfs -du -h /user/grp_edw_rep_prod/backup/${i} | awk '{print $3}' > ${i}.txt 
        for j in `cat ${i}.txt`
	do
		dt=`echo $j | awk -F'/' '{print $6}' | sed 's/ds=//g'`
		(echo "$j" | grep -Eq  ^.*ds=.*$) && path=$j || path="${j}/data"	
		echo "alter table ${i} add if not exists partition (ds='${dt}') location 'hdfs://cerebro-namenode${path}';" >> ${i}.hql
	done
	hive -f ${i}.hql
done
