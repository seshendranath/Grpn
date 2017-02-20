package com.groupon.edw.email

/**
  * Created by aguyyala on 2/16/17.
  */


import com.github.nscala_time.time.Imports._
import org.apache.hadoop.fs.{FileContext, FileSystem, Path}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.joda.time.Days
import scala.annotation.tailrec
import scala.collection.mutable

import EmailCore._
import Utils._


class EmailCore(spark: SparkSession, emailConfig: EmailConfig.Config) {

  import emailConfig._
  import spark.implicits._

  val log = Logger.getLogger(getClass)
  log.setLevel(Level.toLevel(appLogLevel))

  val sql = spark.sql _
  val dfs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
  val dfc = FileContext.getFileContext(spark.sparkContext.hadoopConfiguration)
  val hiveMetaStore = new HiveMetaStoreClient(new HiveConf())
  val eventsMapInverse: Map[String, String] = eventsMap.map(_.swap)
  val sourceLocation = getHiveTableLocation(hiveMetaStore, sourceDb, sourceTable)
  val sourceInputFormat = getHiveInputFormat(hiveMetaStore, sourceDb, sourceTable)

  def runner() = {

    log.info("Kicked Off")
    val (startDt, endDt) = getStartEndTimeStamp(startTimeStamp, endTimeStamp)
    val (eventDatesMap, eventDatesFilesMap) = extractDatesAndFilesToProcess(startDt, endDt)

    log.info(s"EventDatesMap $eventDatesMap")
    val dts = eventDatesMap.values.toList.flatMap(x => x).distinct.sortWith(_ < _).map(x => DateTime.parse(x))
    if (dts.isEmpty) {
      log.info("No Dates to Process. Either the threshold is too high or the Upstream didn't write any files")
      System.exit(1)
    }

    val batches = prepareBatch(dts, batchSize, intervalSize)


    for ((batchStartDate, batchEndDate) <- batches) {
      log.info("Processing batch " + (batchStartDate.toString(yyyy_MM_dd), batchEndDate.toString(yyyy_MM_dd)))

      for (event <- events) {

        log.info("Getting source files for *** " + event)
        val files = getFiles(event, eventDatesMap(event), batchStartDate, batchEndDate, eventDatesFilesMap)
        createDF(event, sourceInputFormat, files)
        log.info("=" * 200)
      }

      log.info("Creating stage tables")
      createStageTables()
      log.info("Stage tables are created")

      log.info("Creating stage tables for Agg Email")
      val stgAggEmail = createAggEmailStage()
      saveDataFrameToHdfs(stgAggEmail.coalesce(outputNumFiles), stgLocation, targetInputFormat)

      log.info("Checking for DDL changes and Staging Table Existence")
      val stgCols = getColsFromDF(stgAggEmail, Array[String]())
      checkAndCreateHiveDDL(hiveMetaStore, targetDb, stgTable, targetInputFormat, stgLocation, stgCols, Array[String]())

      log.info("Merging with agg email")
      val aggEmail = mergeAggEmail(batchStartDate, batchEndDate, offset)
      log.info("Saving the merged data to temp location")

      val fCol: Column = outNoFilesPerCountry.keys.toList.foldLeft(lit(2))((acc, c) =>
        when($"${finalPartCol(1)}" === c, lit(outNoFilesPerCountry(c))).otherwise(acc))
      val aggEmailRePart = aggEmail.repartition(finalPartCol.map(c => col(c)) :+ fCol: _*)
      saveDataFrameToHdfs(aggEmailRePart, tmpLocation, targetInputFormat, finalPartCol)

      log.info("Moving data to final location from temp location")
      moveStageToTargetHdfs()

      log.info("Checking for DDL changes and Table Existence")
      val cols = getColsFromDF(aggEmail, finalPartCol)
      checkAndCreateHiveDDL(hiveMetaStore, targetDb, targetTable, targetInputFormat, targetLocation, cols, finalPartCol)

      log.info("Adding partitions to hive metastore")
      val sendDates: List[DateTime] = DateTime.parse(defaultDate) :: (batchStartDate - offset.days to batchEndDate by 1.day).toList
      aggEmailAddHivePartitions(sendDates, countries.toList)
      log.info("=" * 30 + "Batch Finished" + "=" * 30)

    }

    log.info("=" * 30 + "Process Finished" + "=" * 30)
  }

  def getFiles(event: String, dates: Seq[String], batchStartDate: DateTime, batchEndDate: DateTime,
               eventDatesFilesMap: mutable.Map[(String, String), List[String]]): List[String] = {
    val files = mutable.ListBuffer[List[String]]()
    for (date <- dates) {
      val dt = DateTime.parse(date)
      if (batchStartDate <= dt && dt <= batchEndDate) {
        files += eventDatesFilesMap((event, date))
      }

    }
    files.toList.flatMap(x => x)
  }

  def createDF(event: String, format: String, files: Seq[String]): Unit = {

    log.info(s"Defining source views for $event")

    val df = spark.read.format(format).load(files: _*)
      .filter(s"$sourceCountryColumn in (${seqToQuotedString(countries)})")
      .withColumn(s"$eventDateCol", from_unixtime($"$eventTimeCol" / 1000, yyyy_MM_dd))

    df.createOrReplaceTempView(eventsMap(event))

  }

  def extractDatesAndFilesToProcess(startDt: DateTime, endDt: DateTime) = {
    val eventDatesMap = mutable.Map[String, Seq[String]]()
    val eventDatesFilesMap = mutable.Map[(String, String), List[String]]()

    for (event <- events) {

      val allFiles = dfs.globStatus(new Path(s"$sourceLocation/${sourcePartitionLocation.format(dtPattern, platform, event)}/*"))
        .filter(x => x.getModificationTime > startDt.getMillis && x.getModificationTime <= endDt.getMillis)

      val datesToProcess = allFiles.map(fs => (fs.getLen, raw"\d{4}-\d{2}-\d{2}".r.findFirstIn(fs.getPath.toString)))
        .groupBy { case (size, date) => date }.mapValues(x => x.map { case (size, date) => size }.sum)
        .filter((x) => x._2 > sizeThresholds(event))

      val datesOnly = datesToProcess.keys.flatten.toList

      val filesToProcess = allFiles.filter { fs => datesOnly.contains(raw"\d{4}-\d{2}-\d{2}".r.findFirstIn(fs.getPath.toString).getOrElse("None"))
      }.map(fs => fs.getPath.toString)

      for (f <- filesToProcess) {
        val dt = raw"\d{4}-\d{2}-\d{2}".r.findFirstIn(f).getOrElse("None")

        if (eventDatesFilesMap contains ((event, dt))) {
          eventDatesFilesMap((event, dt)) = eventDatesFilesMap((event, dt)) ::: List(f)
        }
        else {
          eventDatesFilesMap((event, dt)) = List(f)
        }
      }
      eventDatesMap += (event -> datesOnly)


    }
    (eventDatesMap, eventDatesFilesMap)
  }

  def getColsFromDF(df: DataFrame, exclude: Seq[String]) = {
    log.info("Extracting Column Info from DataFrame")
    val cols = mutable.ArrayBuffer[(String, String)]()
    for (column <- df.dtypes) {
      val (col, dataType) = column
      if (!(exclude contains col)) {
        cols += ((col, sparkToHiveDataType(dataType)))
      }
    }
    cols
  }

  def moveStageToTargetHdfs() = {
    val tmpDirs = dfs.globStatus(new Path(tmpLocation + "/*" * finalPartCol.length)).map(fs => fs.getPath.toString)
    for (i <- tmpDirs) {
      val Array(_, partition) = i.split(tmpString)
      hdfsRemoveAndMove(dfs, dfc, i, targetLocation + partition)
    }

  }

  def aggEmailAddHivePartitions(dates: List[DateTime], countries: List[String]) = {

    val parts = mutable.ListBuffer[(List[String], String)]()
    for (date <- dates; country <- countries) {
      val loc = targetLocation + "/" + finalPartCol(0) + "=" + date.toString(yyyy_MM_dd) + "/" + finalPartCol(1) + "=" + country
      val path = new Path(loc)
      if (dfc.util.exists(path)) {
        parts += ((List(date.toString(yyyy_MM_dd), country), loc))
      }
    }
    log.info("Adding Partitions to the Table")
    addHivePartitions(hiveMetaStore, targetDb, targetTable, parts.toList)
  }

  def createStageTables() = {
    createEmailSendStage()
    createEmailDeliveryStage()
    createEmailOpenStage()
    createEmailClickStage()
    createEmailBounceStage()
  }


  def createEmailSendStage() = {
    val qry =
      s"""
         | SELECT
         |      emailSendId
         |     ,SHA2(CONCAT('$emailSalt', emailReceiverAddress), 256) AS emailHash
         |     ,country
         |     ,MIN(emailSubject) AS emailSubject
         |     ,MIN(campaignGroup) AS campaignGroup
         |     ,MIN(businessGroup) AS businessGroup
         | FROM send
         | GROUP BY 1,2,3
       """.stripMargin

    val sendDF = sql(qry)
    sendDF.createTempView("stg_email_send")
    log.info("EmailSendStage:" + qry)

  }

  def createEmailDeliveryStage() = {
    val qry =
      s"""
         | SELECT
         |      emailSendId
         |     ,SHA2(CONCAT('$emailSalt', emailReceiverAddress), 256) AS emailHash
         |     ,country
         |     ,MIN(from_unixtime(CAST(substr(eventTime,1,10) AS INT),'yyyy-MM-dd HH:mm:ss')) AS event_time
         |     ,MIN(event_date) AS event_date
         |     ,MIN(emailName) AS emailName
         | FROM delivery
         | GROUP BY 1,2,3
       """.stripMargin

    val sendDeliveryDF = sql(qry)
    sendDeliveryDF.createTempView("stg_email_delivery")
    log.info("EmailDeliveryStage:" + qry)

  }

  def createEmailOpenStage() = {
    val qry =
      s"""
         | SELECT
         |      emailSendId
         |     ,emailHash
         |     ,country
         |     ,MIN(event_date) AS event_date
         |     ,MIN(userAgent) AS userAgent
         | FROM click
         | WHERE event="${eventsMapInverse("open")}"
         | GROUP BY 1,2,3
      """.stripMargin

    val openDF = sql(qry)
    openDF.createTempView("stg_email_open")
    log.info("EmailOpenStage:" + qry)

  }

  def createEmailClickStage() = {
    val qry =
      s"""
         | SELECT
         |      emailSendId
         |     ,emailHash
         |     ,country
         |     ,MIN(event_date) AS event_date
         |     ,MIN(userAgent) AS userAgent
         |     ,MIN(CASE WHEN clickDestination LIKE '%unsub%' THEN event_date ELSE $defaultDate END) AS unsub_date
         | FROM click
         | WHERE event="${eventsMapInverse("click")}"
         | GROUP BY 1,2,3
      """.stripMargin


    val clickDF = sql(qry)
    clickDF.createTempView("stg_email_click")
    log.info("EmailClickStage:" + qry)

  }

  def createEmailBounceStage() = {
    var qry =
      s"""
         | SELECT
         |      emailSendId
         |     ,SHA2(CONCAT('$emailSalt', emailReceiverAddress), 256) AS emailHash
         |     ,country
         |     ,CASE WHEN bounceCategory IN ('1 Undetermined','40 Generic Bounce','50 Mail Block','100 Challenge-Response'
         |                                   ,'52 Spam Content','54 Relaying Denied','51 Spam Block') THEN 1
         |           WHEN bounceCategory IN ('70 Transient Failure','20 Soft Bounce','24 Timeout','23 Too Large',
         |                                   '25 Admin Failure','60 Auto-Reply','21 DNS Failure','22 Mailbox Full') THEN 2
         |           WHEN bounceCategory IN ('10 Invalid Recipient','90 Unsubscribe','30 Generic Bounce No RCPT') THEN 3
         |           WHEN sourceTopicName LIKE 'msys_fbl%' OR sourceTopicName LIKE 'msys_listunsub%' THEN 4
         |           ELSE NULL END AS  email_bounce_type_id
         |     ,MIN(event_date) as event_date
         | FROM bounce
         | GROUP BY 1,2,3,4
      """.stripMargin


    sql(qry).createTempView("stg_email_bounce_temp")
    log.info("EmailBounceStageTemp:" + qry)

    qry =
      """
        | SELECT
        |       emailSendId
        |      ,emailHash
        |      ,country
        |      ,MIN(CASE WHEN email_bounce_type_id=4 THEN event_date ELSE NULL END) AS complaint_date
        |      ,MIN(CASE WHEN email_bounce_type_id IN (1,2) THEN event_date ELSE NULL END) AS softbounce_date
        |      ,MIN(CASE WHEN email_bounce_type_id=3 THEN event_date ELSE NULL END) AS hardbounce_date
        |      ,MIN(event_date) AS event_date
        | FROM stg_email_bounce_temp
        | GROUP BY 1,2,3
      """.stripMargin


    val bounceDF = sql(qry)
    bounceDF.createTempView("stg_email_bounce")
    log.info("EmailBounceStage:" + qry)
  }

  def createAggEmailStage(): DataFrame = {
    val qry =
      s"""
         | SELECT
         |      u.uuid AS user_uuid,
         |      t.*
         | FROM
         |    (
         |     SELECT
         |          COALESCE(d.emailSendId, s.emailSendId, o.emailSendId, c.emailSendId, b.emailSendId) AS send_id
         |         ,COALESCE(d.emailHash, s.emailHash, o.emailHash, c.emailHash, b.emailHash) as emailHash
         |         ,COALESCE(d.country, s.country, o.country, c.country, b.country) AS country_code
         |         ,d.event_date AS send_date
         |         ,d.event_time AS send_timestamp
         |         ,d.emailName AS email_name
         |         ,s.emailSubject AS email_subject
         |         ,s.campaignGroup AS campaign_group
         |         ,s.businessGroup AS business_group
         |         ,o.userAgent AS first_open_user_agent
         |         ,c.userAgent AS first_click_user_agent
         |         ,o.event_date AS first_open_date
         |         ,c.event_date AS first_click_date
         |         ,c.unsub_date AS first_unsub_date
         |         ,b.event_date AS first_bounce_date
         |         ,b.complaint_date AS first_complaint_date
         |         ,b.softbounce_date AS first_softbounce_date
         |         ,b.hardbounce_date AS first_hardbounce_date
         |     FROM stg_email_bounce b
         |     FULL OUTER JOIN stg_email_click c
         |     ON b.emailSendId = c.emailSendId AND b.emailHash = c.emailHash AND b.country = c.country
         |     FULL OUTER JOIN stg_email_open o
         |     ON COALESCE(c.emailSendId, b.emailSendId) = o.emailSendId
         |        AND COALESCE(c.emailHash, b.emailHash) = o.emailHash AND COALESCE(c.country, b.country) = o.country
         |     FULL OUTER JOIN stg_email_send s
         |     ON COALESCE(o.emailSendId, c.emailSendId, b.emailSendId) = s.emailSendId
         |        AND COALESCE(o.emailHash, c.emailHash, b.emailHash) = s.emailHash AND COALESCE(o.country, c.country, b.country) = s.country
         |     FULL OUTER JOIN stg_email_delivery d
         |     ON COALESCE(s.emailSendId, o.emailSendId, c.emailSendId, b.emailSendId) = d.emailSendId
         |        AND COALESCE(s.emailHash, o.emailHash, c.emailHash, b.emailHash) = d.emailHash
         |        AND COALESCE(s.country, o.country, c.country, b.country) = d.country
         |    ) t
         | LEFT OUTER JOIN $dimUserTbl u
         | ON t.emailHash = u.encrypted_login_email AND t.country_code = u.country_code
      """.stripMargin

    val stgAggEmailDF = sql(qry)
    stgAggEmailDF.cache()
    stgAggEmailDF.createTempView("stg_agg_email")
    log.info("AggEmailStage:" + qry)
    stgAggEmailDF

  }

  def mergeAggEmail(startDate: DateTime, endDate: DateTime, offset: Int) = {
    val qry =
      s"""
         | SELECT
         |      user_uuid AS user_uuid
         |     ,send_id AS send_id
         |     ,emailHash AS emailHash
         |     ,country_code AS country_code
         |     ,send_date AS send_date
         |     ,send_timestamp AS send_timestamp
         |     ,email_name AS email_name
         |     ,email_subject AS email_subject
         |     ,campaign_group AS campaign_group
         |     ,business_group AS business_group
         |     ,first_open_user_agent AS first_open_user_agent
         |     ,first_click_user_agent AS first_click_user_agent
         |     ,first_open_date AS first_open_date
         |     ,first_click_date AS first_click_date
         |     ,first_unsub_date AS first_unsub_date
         |     ,first_bounce_date AS first_bounce_date
         |     ,first_complaint_date AS first_complaint_date
         |     ,first_softbounce_date AS first_softbounce_date
         |     ,first_hardbounce_date AS first_hardbounce_date
         |     ,CASE WHEN a.first_open_date = a.send_date THEN 1 ELSE 0 END AS sd_open_cnt
         |     ,CASE WHEN a.first_click_date = a.send_date THEN 1 ELSE 0 END AS sd_click_cnt
         |     ,CASE WHEN a.first_unsub_date = a.send_date THEN 1 ELSE 0 END AS sd_unsub_cnt
         |     ,CASE WHEN datediff(a.first_open_date, a.send_date) BETWEEN 0 AND 2 THEN 1 ELSE 0 END AS d3_open_cnt
         |     ,CASE WHEN datediff(a.first_click_date, a.send_date) BETWEEN 0 AND 2 THEN 1 ELSE 0 END AS d3_click_cnt
         |     ,CASE WHEN datediff(a.first_unsub_date, a.send_date) BETWEEN 0 AND 2 THEN 1 ELSE 0 END AS d3_unsub_cnt
         |     ,CASE WHEN datediff(a.first_open_date, a.send_date) BETWEEN 0 AND 3 THEN 1 ELSE 0 END AS d4_open_cnt
         |     ,CASE WHEN datediff(a.first_click_date, a.send_date) BETWEEN 0 AND 3 THEN 1 ELSE 0 END AS d4_click_cnt
         |     ,CASE WHEN datediff(a.first_unsub_date, a.send_date) BETWEEN 0 AND 3 THEN 1 ELSE 0 END AS d4_unsub_cnt
         |     ,CASE WHEN datediff(a.first_open_date, a.send_date) BETWEEN 0 AND 6 THEN 1 ELSE 0 END AS d7_open_cnt
         |     ,CASE WHEN datediff(a.first_click_date, a.send_date) BETWEEN 0 AND 6 THEN 1 ELSE 0 END AS d7_click_cnt
         |     ,CASE WHEN datediff(a.first_unsub_date, a.send_date) BETWEEN 0 AND 6 THEN 1 ELSE 0 END AS d7_unsub_cnt
         | FROM
         |    (
         |      SELECT
         |           COALESCE(f.user_uuid, s.user_uuid) AS user_uuid
         |          ,COALESCE(f.send_id, s.send_id) AS send_id
         |          ,COALESCE(f.emailHash, s.emailHash) AS emailHash
         |          ,COALESCE(f.country_code, s.country_code) AS country_code
         |          ,LEAST(f.send_date, s.send_date, '$defaultDate') AS send_date
         |          ,LEAST(f.send_timestamp, s.send_timestamp) AS send_timestamp
         |          ,COALESCE(f.email_name, s.email_name) AS email_name
         |          ,COALESCE(f.email_subject, s.email_subject) AS email_subject
         |          ,COALESCE(f.campaign_group, s.campaign_group) AS campaign_group
         |          ,COALESCE(f.business_group, s.business_group) AS business_group
         |          ,COALESCE(f.first_open_user_agent, s.first_open_user_agent) AS first_open_user_agent
         |          ,COALESCE(f.first_click_user_agent, s.first_click_user_agent) AS first_click_user_agent
         |          ,LEAST(f.first_open_date, s.first_open_date) AS first_open_date
         |          ,LEAST(f.first_click_date, s.first_click_date) AS first_click_date
         |          ,LEAST(f.first_unsub_date, s.first_unsub_date) AS first_unsub_date
         |          ,LEAST(f.first_bounce_date, s.first_bounce_date) AS first_bounce_date
         |          ,LEAST(f.first_complaint_date, s.first_complaint_date) AS first_complaint_date
         |          ,LEAST(f.first_softbounce_date, s.first_softbounce_date) AS first_softbounce_date
         |          ,LEAST(f.first_hardbounce_date, s.first_hardbounce_date) AS first_hardbounce_date
         |      FROM
         |          (
         |            SELECT *
         |            FROM $targetDb.$targetTable
         |            WHERE (${finalPartCol(0)} BETWEEN '${startDate.minusDays(offset).toString(yyyy_MM_dd)}' AND '${endDate.toString(yyyy_MM_dd)}'
         |                    OR ${finalPartCol(0)} = '$defaultDate')
         |                  AND ${finalPartCol(0)} IN (${seqToQuotedString(countries)})
         |          ) f
         |      FULL OUTER JOIN stg_agg_email s
         |      ON s.send_id = f.send_id  AND s.emailHash = f.emailHash AND s.country_code = f.country_code
         |    ) a
      """.stripMargin

    log.info("Final query" + qry)
    val df = sql(qry)
    df
  }

}

object EmailCore {

  /**
    *
    * @param dates        List of dates to process
    * @param size         Max number of days in single batch
    * @param intervalDays interval days between adjacent date time to consider in the same batch
    * @return Seq(Batch startDate, Batch endDate)
    */
  def prepareBatch(dates: Seq[DateTime], size: Int, intervalDays: Int): Seq[(DateTime, DateTime)] = {

    val dts = dates.toList.sortWith(_ < _)

    @tailrec
    def loop(acc: List[(DateTime, DateTime)], batchStartDt: DateTime, prevDt: DateTime, dt: List[DateTime]): List[(DateTime, DateTime)] = {

      dt match {
        case Nil => (batchStartDt, prevDt) :: acc
        case curDt :: remainingDts =>
          if (Days.daysBetween(prevDt, curDt).getDays <= intervalDays && Days.daysBetween(batchStartDt, curDt).getDays <= size) {
            loop(acc, batchStartDt, curDt, remainingDts)
          }
          else {
            loop((batchStartDt, prevDt) :: acc, curDt, curDt, remainingDts)
          }
      }

    }
    loop(Nil, dts.head, dts.head, dts.tail)

  }

}
