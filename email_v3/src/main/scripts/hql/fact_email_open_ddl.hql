DROP TABLE IF EXISTS fact_email_open;
CREATE TABLE IF NOT EXISTS fact_email_open(
  appVersion string,
  channel string,
  clientPlatform string,
  consumerId string,
  countryCampaignID string,
  division string,
  event string,
  eventTime string,
  platform string,
  rawEvent string,
  sourceType string,
  websiteId string,
  emailsendid string,
  bcookie string,
  campaign string,
  emailhash string,
  medium string,
  useragent string)
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
  '/user/grp_gdoop_edw_etl_dev/email/fact_email_open';