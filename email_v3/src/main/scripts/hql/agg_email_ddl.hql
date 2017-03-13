CREATE EXTERNAL TABLE `agg_email`(
  `user_uuid` string COMMENT 'user_uuid',
  `send_id` string COMMENT 'send_id',
  `emailhash` string COMMENT 'emailHash',
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
  `first_hardbounce_date` string COMMENT 'first_hardbounce_date',
  `sd_open_cnt` int COMMENT 'sd_open_cnt',
  `sd_click_cnt` int COMMENT 'sd_click_cnt',
  `sd_unsub_cnt` int COMMENT 'sd_unsub_cnt',
  `d3_open_cnt` int COMMENT 'd3_open_cnt',
  `d3_click_cnt` int COMMENT 'd3_click_cnt',
  `d3_unsub_cnt` int COMMENT 'd3_unsub_cnt',
  `d4_open_cnt` int COMMENT 'd4_open_cnt',
  `d4_click_cnt` int COMMENT 'd4_click_cnt',
  `d4_unsub_cnt` int COMMENT 'd4_unsub_cnt',
  `d7_open_cnt` int COMMENT 'd7_open_cnt',
  `d7_click_cnt` int COMMENT 'd7_click_cnt',
  `d7_unsub_cnt` int COMMENT 'd7_unsub_cnt')
PARTITIONED BY (
  `send_date` string COMMENT 'send_date',
  `country_code` string COMMENT 'country_code')
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://gdoop-staging-namenode/user/grp_gdoop_edw_etl_dev/email/res';