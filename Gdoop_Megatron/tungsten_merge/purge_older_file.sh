#!/bin/bash
usage="Usage: purge_older_file.sh [days] [owner] [path]"

if [ ! "$3" ]
then
  echo $usage
  exit 1
fi

if [ $3 == '/' ]; then
   echo " can not delete from root"
   exit 1
fi
now=$(date +%s)
hadoop fs -ls -R  $3| grep "^d" | while read f; do
  dir_date=`echo $f | awk '{print $6, $7}'`
  owner_name=`echo $f | awk '{print $3}'`
  filename=`echo $f | awk '{print $8}'`
  difference=$(( ( $now - $(date -d "$dir_date" +%s) ) / (24 * 60 * 60 ) ))
  if [ $difference -gt $1 ] && [ "$owner_name" == "$2" ]; then
	number_of_childdirs=`hadoop fs -ls $filename |grep "^d" |wc -l`
	if [ $number_of_childdirs -eq 0 ]; then
	        echo hadoop fs -rm -R $filename
	        if [ "$4"  != "dryrun" ]; then
        	    hadoop fs -rm -R $filename
        	fi
    fi
  fi
done
