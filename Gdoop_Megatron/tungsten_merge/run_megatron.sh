#!/bin/sh
export PYTHONPATH=/usr/local/lib/python2.7/site-packages/zombie_runner

if test "$#" -ne 3; then
    echo "Illegal number of parameters"
    echo "Usage: run_megatron.sh <process_type> <execution_env> <service_path>"
    echo "Supported command options.  process_type : [sqoop|full_load|load|merge],  execution_env: [dev|prod],  service_path: relative path to service config folder"
    exit 1
else
    run_mode=$1
    prod_mode=$2
    service_path=$3
    
    service_name=$(basename $service_path)
    ds=`date "+%Y-%m-%d"`
    hh=`date "+%H-%M-%S"`
    run_id="${service_name}_${run_mode}_${ds}-${hh}"

    bin=`dirname "$0"`
    service_conf_path="${bin}/${service_path}"
    bin=`cd "$bin"; pwd`

    cmd="python $bin/main.py --run_mode=${run_mode} --tungsten_conf=${service_conf_path}/tung.yml --db_conf=${service_conf_path}/mysql_${prod_mode}.yml --workflow_id=${run_id}  -vv"
    
    output_log="/tmp/megatron_${run_id}.out"
    echo $output_log
    echo $cmd >> $output_log
    $cmd >> $output_log 2>&1

    ret=$?
    exit $ret
fi

