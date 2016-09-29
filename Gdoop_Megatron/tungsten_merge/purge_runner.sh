#!/bin/sh
export PYTHONPATH=/usr/local/lib/python2.7/site-packages/zombie_runner
/home/svc_meg_prod/megatron/tungsten_merge/purge_api.py "$@"

