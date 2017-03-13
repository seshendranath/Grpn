DROP TABLE IF EXISTS fact_email_delivery;
CREATE TABLE IF NOT EXISTS fact_email_delivery(
  dlvDestinationIP string,
  dlvSourceIP string,
  dlvSize string,
  dsnAction string,
  dsnMTA string,
  emailHash string,
  emailName string,
  emailReceiverAddress string,
  emailSendId string,
  emailType string,
  errorMessage string,
  event string,
  hostIP string,
  platform string,
  rawEvent string,
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
  '/user/grp_gdoop_edw_etl_dev/email/fact_email_delivery';