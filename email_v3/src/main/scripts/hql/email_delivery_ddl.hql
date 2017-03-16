DROP TABLE IF EXISTS email_delivery;
CREATE TABLE IF NOT EXISTS email_delivery(
bounceCategory string,
hostIP string,
dlvSourceIP string,
dlvDestinationIP string,
dlvSize string,
virtualMta string,
emailName string,
emailHash string,
emailSendId string,
testGroup string,
sendAddress string,
emailReceiverAddress string,
address string,
dsnAction string,
status string,
errorMessage string,
dsnMTA string,
emailType string,
userId string,
platform string)
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
  '/user/grp_gdoop_edw_etl_prod/prod_groupondw/email_delivery';