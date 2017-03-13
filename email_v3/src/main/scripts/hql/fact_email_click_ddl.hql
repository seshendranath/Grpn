DROP TABLE IF EXISTS fact_email_click;
CREATE TABLE IF NOT EXISTS fact_email_click(
  emailsendid string,
  emailhash string,
  bcookie string,
  appVersion string,
  clickedon string,
  clickdestination string,
  division string,
  position string,
  clicksection string,
  campaign string,
  medium string,
  channel string,
  useragent string,
  clientPlatform string,
  consumerId string,
  dealPermalink string,
  sourceType string,
  websiteId string,
  event string,
  eventtime string)
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
  '/user/grp_gdoop_edw_etl_dev/email/fact_email_click';