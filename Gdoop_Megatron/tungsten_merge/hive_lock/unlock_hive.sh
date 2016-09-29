#hive -S -e "use groupon_orders; show locks groupon_production_orders_final extended;" | grep LOCK_TIME:| awk -F':' '{print substr($2,1,10)}'

#!/bin/sh

min=$1

for i in `ps xo etime,cmd | awk '$1>"'$min':00" {$1="";print $0}' | grep -E 'zombie_runner.*merge' | awk '{print $4}' | awk -F'/' 'NF-1==7 {print $0"/*.log"}'`
do
tail -1 $i | grep 'conflicting lock present' | awk '{print $15}' | awk -F'@' '{ cmd="hive -e \"use "$1";""unlock table "$2"\""; print cmd; system(cmd) }'
done
