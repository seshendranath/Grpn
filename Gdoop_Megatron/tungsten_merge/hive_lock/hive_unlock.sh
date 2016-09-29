#!/bin/sh

min=$1
sec=$(($min * 60))

for i in `ps xo etime,cmd | awk '$1>"'$min':00" {$1="";print $0}' | grep -E 'zombie_runner.*load' | awk '{print $4}' | awk -F'/' 'NF-1==7 {print $0"/*.log"}'`
do

hive_lock=9999999999

for j in `tail -1 $i | grep 'conflicting lock present' | awk '{print $15}' | awk -F'@' '{ cmd="hive -S -e \"use "$1";show locks "$2" extended;\" | grep LOCK_TIME | awk '\''{print substr($0,11,10)}'\''"; print cmd; system(cmd) }'`
do

if [[ $j -lt $hive_lock ]]; then
  hive_lock=$j
fi

done

if [[ $hive_lock != 9999999999 ]]; then

  lock_time=$((`date +%s` - $hive_lock))
  echo "Hive Lock: "$hive_lock
  echo "Lock Time: "$lock_time

  if [[ $lock_time -gt $sec ]]; then
      tail -1 $i | grep 'conflicting lock present' | awk '{print $15}' | awk -F'@' '{ cmd="hive -e \"use "$1";""unlock table "$2"\""; print cmd; system(cmd) }'
  fi
fi
done
