td_schema=$1
td_table=$2
inc_col=$3
split_size_in_days=$4
hive_table=${2}_full

stg_schema=sandbox
db=tdwc
user=B_TDHADOOPMOVER
password=th6jwzhq50

min_max=$(bteq << EOF 2>&1 
.LOGON ${db}/${user},${password}
select min(${inc_col}),max(${inc_col}) from ${td_schema}.${td_table} $where_clause;
.LOGOFF;
.QUIT;
.EXIT;
EOF
)

#min_max="20141230 20160731"
min_dt=`echo $min_max | sed 's/[\/,-]//g' | awk '{print $1}'`
max_dt=`echo $min_max | sed 's/[\/,-]//g' | awk '{print $2}'`

echo $min_dt,$max_dt

out=$(bteq << EOF 2>&1
.LOGON ${db}/${user},${password}
show table ${td_schema}.${td_table};
.LOGOFF;
.QUIT;
.EXIT;
EOF
)

echo "$out"
pi=`echo "$out" | grep -P 'PRIMARY INDEX \(.*\)' -o | cut -d "(" -f2 | cut -d ")" -f1`

out=$(bteq << EOF 2>&1
.LOGON ${db}/${user},${password}
DROP TABLE ${stg_schema}.${td_table}_stg;
CREATE TABLE ${stg_schema}.${td_table}_stg AS ${td_schema}.${td_table} WITH NO DATA PRIMARY INDEX(${pi});
.LOGOFF;
.QUIT;
.EXIT;
EOF
)

echo "$out"

i=0
dt=$min_dt
date=`date "--date=$min_dt" +%Y-%m-%d`
from_clause="( "
while [ $dt -lt $max_dt ]
do

i=`expr $i + 1`
inc_date=`date "--date=${dt}+${split_size_in_days} day" +%Y-%m-%d`
where="where ${inc_col} >= '${date}' and ${inc_col} < '${inc_date}'"
echo $where
date=${inc_date}
dt=${inc_date//-/}


out=$(bteq << EOF 2>&1
.LOGON ${db}/${user},${password}
DELETE ${stg_schema}.${td_table}_stg ALL;
INSERT INTO ${stg_schema}.${td_table}_stg SELECT * from ${td_schema}.${td_table} $where;
.LOGOFF;
.QUIT;
.EXIT;
EOF
)
ret_code=$?
echo "$out"

if [[ "${ret_code}" -eq 0  ]]; then
      echo "Staging Table Created completed successfully"
else
      echo "Staging Table Creation FAILED"
      exit 1
fi


current_Time=`date +%Y%m%d%H%M%S`

echo "python /home/svc_replicator_prod/Replicator/scripts/RunReplicator.py import --table=${stg_schema}.${td_table}_stg --hive_table=${stg_schema}.${hive_table}_${i} -v | tee /tmp/${td_schema}_${td_table}_${current_Time}.log 2>&1"

python /home/svc_replicator_prod/Replicator/scripts/RunReplicator.py import --table=${stg_schema}.${td_table}_stg --hive_table=${stg_schema}.${hive_table}_${i} -v | tee /tmp/${td_schema}_${td_table}_${current_Time}.log 2>&1

ret_code=${PIPESTATUS[0]}
echo "RETCODE:"$ret_code

if [[ "${ret_code}" -eq 0  ]]; then
      echo "Table Copy completed successfully"
else
      echo "Table Copy FAILED"
      exit 1
fi

from_clause+="select * from ${stg_schema}.${td_table}_full_${i} UNION ALL "
drop_clause+="DROP TABLE ${stg_schema}.${td_table}_full_${i};\n"
hdfs_rm+="dfs -rm -r \/user\/grp_edw_rep_prod\/backup\/${stg_schema}.${td_table}_full_${i};\n"

done

from_clause+=")"
from_clause=`echo "$from_clause" | awk -F'UNION ALL )' '{print $1}'` 
from_clause+=") a"
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
