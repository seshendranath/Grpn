DROP TABLE IF EXISTS fact_email_send;
CREATE TABLE IF NOT EXISTS fact_email_send(
  appVersion string,
  businessGroup string,
  campaignGroup string,
  campaignId string,
  consumerId string,
  dealList string,
  dealSource string,
  emailHash string,
  emailName string,
  emailReceiverAddress string,
  emailSubject string,
  emailType string,
  event string,
  messageId string,
  platform string,
  sourceTopicName string,
  virtualMta string)
PARTITIONED BY (
  event_date string,
  country string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '/user/grp_gdoop_edw_etl_dev/email/fact_email_send';