#!/bin/bash
current_time=$(date +%s)
ps axo lstart=,pid=,cmd= |
    grep -E 'zombie_runner.*merge' |
    while read line
    do
        # 60 * 60 is one hour, multiply additional or different factors for other thresholds 
        if (( $(date -d "${line:0:25}" +%s) < current_time - 60 * 60 ))
        then
            echo $line
        fi
    done
