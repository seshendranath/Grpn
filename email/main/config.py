#!/usr/bin/env python

import os
import datetime

global args


def get(arg):
    return args.get(arg, None)


def set_arg(arg, val):
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


def get_event_date():
    if args['--eventDate']:
        return args['--eventDate']
    else:
        return get_yesterday_date()


def get_offset():
    return args.get('--offset')


def get_default_countries():
    return ['US', 'CA', 'BE', 'FR', 'DE', 'IE', 'IT', 'NL', 'PL', 'ES', 'AE', 'UK', 'JP', 'AU', 'NZ']


def get_countries():
    if args.get('--countries'):
        return [i.upper() for i in args.get('--countries').split(',')]
    return [i.upper() for i in get_default_countries()]


def get_num_countries():
    return len(get_default_countries())


def get_stage_location():
    return "/user/grp_gdoop_edw_etl_dev/email/stg/"


def get_final_location():
    return "/user/grp_gdoop_edw_etl_dev/email/res/"


def get_tmp_location():
    return "/user/grp_gdoop_edw_etl_dev/email/tmp/"


def get_stg_part_cols():
    return ["event_date", get_source_country_column()]


def get_final_part_cols():
    return ["send_date", "country_code"]


def get_source_partition_location():
    return "/eventDate={event_date}/platform={platform}/eventDestination={event_dest}"


def get_email(dest):
    if dest == 'send':
        return 'emailSend'
    elif dest == 'delivery':
        return 'emailDelivery'
    elif dest == 'open':
        return 'emailOpenHeader'
    elif dest == 'click':
        return 'emailClick'
    elif dest == 'bounce':
        return 'emailBounce'


def get_event_destinations():
    if args.get('--eventDestination'):
        return args.get('--eventDestination').split(',')
    return [get_email('send'), get_email('delivery'), get_email('click'), get_email('bounce')]


def get_email_delivery_cols():
    delivery_cols = ["emailsendid", "emailreceiveraddress", "emailname", "eventtime", "country"]
    return delivery_cols


def get_email_send_cols():
    send_columns = ["emailsendid", "emailreceiveraddress", "emailsubject", "campaigngroup",
                    "businessgroup", "eventtime", "country"]
    return send_columns


def get_email_click_cols():
    click_columns = ["emailhash", "emailsendid", "useragent", "eventtime", "clickDestination", "event", "country"]
    return click_columns


def get_email_bounce_cols():
    bounce_columns = ["emailsendid", "emailreceiveraddress", "bouncecategory", "sourcetopicname", "eventtime",
                      "country"]

    return bounce_columns


def get_email_salt():
    return 'ph5p6uTezuwr4c8aprux'


def get_final_table():
    return "svc_edw_dev_db.fact_email"


def get_source_country_column():
    return "country"


def get_event_time_col():
    return "eventTime"


def get_platform():
    return 'email'


def get_filter_condition(event_destination):
    if event_destination in (get_email('send'), get_email('delivery'), get_email('bounce')):
        return "emailsendid not in ('','ffffffff-ffff-ffff-ffff-ffffffffffff') and emailreceiveraddress != ''"
    else:
        return "emailsendid not in ('','ffffffff-ffff-ffff-ffff-ffffffffffff') and emailhash != ''"


def get_current_timestamp():
    return '{:%Y%m%d%H%M%S}'.format(datetime.datetime.now())


def get_current_date():
    return '{:%Y-%m-%d}'.format(datetime.date.today())


def get_yesterday_date():
    return '{:%Y-%m-%d}'.format(datetime.date.today() - datetime.timedelta(1))


def get_input_format():
    return 'parquet'


def get_stg_output_format():
    return 'parquet'


def get_final_output_format():
    return 'orc'


def get_output_num_files():
    return 200


def load_conf(arg):
    global args
    args = {}
    args.update(arg)

    args['input_format'] = get_input_format()
    args['stg_output_format'] = get_stg_output_format
    args['final_output_format'] = get_final_output_format()
    args['output_num_files'] = get_output_num_files()

    args['source'] = get_source()
    args['target'] = get_target()
    args['source_db'] = get_source_db()
    args['source_table'] = get_source_table()
    args['target_db'] = get_target_db()
    args['target_table'] = get_target_table()

    args['final_table'] = get_final_table()

    args['source_country_column'] = get_source_country_column()

    args['eventDate'] = get_event_date()
    args['offset'] = get_offset()
    args['platform'] = get_platform()
    args['eventDestinations'] = get_event_destinations()
    args['countries'] = get_countries()
    args['num_countries'] = get_num_countries()

    args['source_partition_location'] = get_source_partition_location()
    args['stage_location'] = get_stage_location()
    args['final_location'] = get_final_location()
    args['tmp_location'] = get_tmp_location()
    args['stg_part_col'] = get_stg_part_cols()
    args['final_part_col'] = get_final_part_cols()
    args['event_time_col'] = get_event_time_col()

    args[get_email('delivery') + '_cols'] = get_email_delivery_cols()
    args[get_email('send') + '_cols'] = get_email_send_cols()
    args[get_email('click') + '_cols'] = get_email_click_cols()
    args[get_email('bounce') + '_cols'] = get_email_bounce_cols()

    args['email_salt'] = get_email_salt()

    return args
