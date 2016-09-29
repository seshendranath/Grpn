#!/bin/sh

to=${1}
if [ "$to" != "" ]; then
    to="--to=${to}"
    exit
fi

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

export PYTHONPATH=/usr/local/lib/python2.7/site-packages/zombie_runner
export ZOMBIERC=$bin/tungsten_merge_zrc
export ODBCINI=~/.odbc.ini


cmd="python $bin/../validate_data.py  --tungsten_conf=$bin/conf/tung.yml --db_conf=$bin/conf/mysql.yml ${to}"

echo $cmd
$cmd

ret=$?
exit $ret


