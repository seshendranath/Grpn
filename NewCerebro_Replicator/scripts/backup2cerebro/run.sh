#!/bin/sh

LOGSDIR=/var/groupon/log

read -t 3 CMD
if [ x"" == x"$CMD" ];
then
   echo "Exiting. No input" 1>&2
   exit 1
fi

PATH_TO_BKUP=/home/svc_replicator_prod/Replicator/scripts/backup2cerebro

#TABLE_NM=$($ECHO $CMD | $AWK '/--table (\w+,\w+) / {print $2}' | $SED 's/,/./')

if [ x"" == x"$RUN_DATE" ];
then
   RUN_DATE=$(/bin/date +%Y%m%d)
fi
#echo $CMD
RV=`/usr/local/bin/perl $PATH_TO_BKUP/hdfs_exim_V1.pl $CMD`
RC=$?
echo "$RV"
exit $RC
