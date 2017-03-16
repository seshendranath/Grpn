DROP TABLE IF EXISTS email_open;
CREATE TABLE IF NOT EXISTS email_open(
appVersion string,
bcookie string,
brand string,
campaign string,
channel string,
clientPlatform string,
consumerId string,
countryCampaignID string,
dealUUID string,
division string,
emailHash string,
emailSendId string,
medium string,
platform string,
position string,
userAgent string,
websiteId string)
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
  '/user/grp_gdoop_edw_etl_prod/prod_groupondw/email_open';