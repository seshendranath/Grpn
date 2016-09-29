#!/usr/bin/env python

import os
import datetime

hive_keywords = ['comment', 'bucket', 'location']

global args


def get(arg):
    return args.get(arg, None)


def set(arg, val):
    args[arg] = val


def get_work_dir():
    cur_dir = os.path.expanduser('.')
    work_dir = os.path.join(cur_dir, 'logs')
    f_work_dir = args.get('--work_dir', work_dir)
    if not os.path.exists(f_work_dir):
        os.makedirs(f_work_dir)
    return f_work_dir


def get_source():
    return args.get('--source')


def get_target():
    return args.get('--target')


def get_source_db():
    return args.get('--source').split('.')[0]


def get_source_table():
    return args.get('--source').split('.')[1]


def get_target_db():
    return args.get('--target').split('.')[0]


def get_target_table():
    return args.get('--target').split('.')[1]


def get_current_timestamp():
    return '{:%Y%m%d%H%M%S}'.format(datetime.datetime.now())


def load_conf(arg):
    global args
    args = {}
    args.update(arg)
    args['source_db'] = get_source_db()
    args['source_table'] = get_source_table()
    args['target_db'] = get_target_db()
    args['target_table'] = get_target_table()
    args['hql_file_name'] = get_work_dir()+"/"+get('--target')+get_current_timestamp()+".hql"

    return args
