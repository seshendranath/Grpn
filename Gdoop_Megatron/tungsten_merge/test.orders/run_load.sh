#!/bin/sh
ds=${1:-`date "+%Y-%m-%d"`}
hh=${2:-`date "+%H-%M-%S"`}
run_id="load_orders_${ds}-${hh}"

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

export PYTHONPATH=/usr/local/lib/python2.7/site-packages/zombie_runner
export ZOMBIERC=$bin/tungsten_merge_zrc
export ODBCINI=~/.odbc.ini


cmd="python $bin/../main.py --run_mode=load  --tungsten_conf=$bin/conf/tung.yml --db_conf=$bin/conf/mysql.yml --workflow_id=${run_id}   -vv"

echo /tmp/tung_${run_id}.out
echo $cmd >> /tmp/tung_${run_id}.out
$cmd >> /tmp/tung_${run_id}.out 2>&1

ret=$?
exit $ret


