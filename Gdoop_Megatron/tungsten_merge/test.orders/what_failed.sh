#!/bin/sh

date_id=${1:-`date "+%Y-%m-%d" -d ' -1 day'`}
to=${2}
if [ "$to" != "" ]; then
    to="--to=${to}"
    exit
fi

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

export PYTHONPATH=/usr/local/lib/python2.7/site-packages/zombie_runner
export ZOMBIERC=$bin/tungsten_merge_zrc
export ODBCINI=~/.odbc.ini


cmd="python $bin/../what_failed.py  --tungsten_conf=$bin/conf/tung.yml --db_conf=$bin/conf/mysql.yml --date_id=$date_id  ${to}"

echo $cmd
$cmd

ret=$?
exit $ret


