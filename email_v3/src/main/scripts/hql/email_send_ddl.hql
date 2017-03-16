DROP TABLE IF EXISTS email_send;
CREATE TABLE IF NOT EXISTS email_send(
additionalEmailAdrress string,
appVersion string,
businessGroup string,
campaignGroup string,
campaignId string,
consumerId string,
dealList string,
dealSource string,
emailFrom string,
emailHash string,
emailName string,
emailReceiverAddress string,
emailSendId string,
emailSenderScore string,
emailSubject string,
emailType string,
eventTime string,
injector string,
locale string,
messageId string,
messageRequestTime string,
platform string,
subscriptionListId string,
userId string,
variantName string,
virtualMta string
  )
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
  '/user/grp_gdoop_edw_etl_prod/prod_groupondw/email_send';