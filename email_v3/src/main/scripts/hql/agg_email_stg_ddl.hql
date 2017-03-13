CREATE EXTERNAL TABLE `agg_email_stg`(
  `user_uuid` string COMMENT 'user_uuid',
  `send_id` string COMMENT 'send_id',
  `emailhash` string COMMENT 'emailHash',
  `country_code` string COMMENT 'country_code',
  `send_date` string COMMENT 'send_date',
  `send_timestamp` string COMMENT 'send_timestamp',
  `email_name` string COMMENT 'email_name',
  `email_subject` string COMMENT 'email_subject',
  `campaign_group` string COMMENT 'campaign_group',
  `business_group` string COMMENT 'business_group',
  `first_open_user_agent` string COMMENT 'first_open_user_agent',
  `first_click_user_agent` string COMMENT 'first_click_user_agent',
  `first_open_date` string COMMENT 'first_open_date',
  `first_click_date` string COMMENT 'first_click_date',
  `first_unsub_date` string COMMENT 'first_unsub_date',
  `first_bounce_date` string COMMENT 'first_bounce_date',
  `first_complaint_date` string COMMENT 'first_complaint_date',
  `first_softbounce_date` string COMMENT 'first_softbounce_date',
  `first_hardbounce_date` string COMMENT 'first_hardbounce_date')
PARTITIONED BY (
  `run_id` string COMMENT 'run_id')
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://gdoop-staging-namenode/user/grp_gdoop_edw_etl_dev/email/res_stg';