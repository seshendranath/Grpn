package com.groupon.edw.email

/**
  * Created by aguyyala on 2/16/17.
  */

import scopt.OptionParser


object EmailConfig {


  def parseCmdLineArguments(args: Array[String]) = {

    val parser = new OptionParser[Config]("EmailProject") {
      head("Email")
      opt[String]("sourceDb") action ((x, c) => c.copy(sourceDb = x)) text "Source DB"
      opt[String]("sourceTable") action ((x, c) => c.copy(sourceTable = x)) text "Source Table"
      opt[String]("targetDb") action ((x, c) => c.copy(targetDb = x)) text "Target DB"
      opt[String]("targetTable") action ((x, c) => c.copy(targetTable = x)) text "Target DB"
      opt[String]("startTimestamp") action ((x, c) => c.copy(startTimeStamp = Some(x))) text "Start TimeStamp: yyyy-MM-dd HH:mm:ss"
      opt[String]("endTimestamp") action ((x, c) => c.copy(endTimeStamp = Some(x))) text "End TimeStamp: yyyy-MM-dd HH:mm:ss"
      opt[Seq[String]]("countries") action ((x, c) => c.copy(countries = x)) text "Countries to be processed"
      opt[Int]("batchSize") action ((x, c) => c.copy(batchSize = x)) text "Batch Size in each run"

      opt[Unit]("debug").action((_, c) =>
        c.copy(debug = true)).text("Debug Flag")

      help("help").text("Prints Usage Text")

      override def showUsageOnError = true

      override def errorOnUnknownArgument = true
    }
    parser.parse(args, Config()).getOrElse(Config())

  }


  case class Config(sourceDb: String = "grp_gdoop_pde",
                    sourceTable: String = "junohourly",
                    targetDb: String = "svc_edw_dev_db",
                    targetTable: String = "agg_email",
                    stgTable: String = "agg_email_stg",
                    startTimeStamp: Option[String] = None,
                    endTimeStamp: Option[String] = None,
                    countries: Seq[String] = countries,
                    debug: Boolean = false,
                    targetInputFormat: String = "orc",
                    stgInputFormat: String = "csv",
                    sourceCountryColumn: String = "country",
                    sourcePartitionLocation: String = "/eventDate=%s/platform=%s/eventDestination=%s",
                    platform: String = "email",
                    events: Array[String] = events,
                    eventsMap: Map[String, String] = eventsMap,
                    eventDateCol: String = "event_date",
                    eventTimeCol: String = "eventTime",
                    targetLocation: String = targetLocation,
                    finalPartCol: Array[String] = Array("send_date", "country_code"),
                    stgPartCol: Array[String] = Array("run_id"),
                    tmpString: String = tmpString,
                    stgString: String = stgString,
                    stgLocation: String = targetLocation + stgString,
                    tmpLocation: String = targetLocation + tmpString,
                    emailSalt: String = "ph5p6uTezuwr4c8aprux",
                    defaultNumCountries: Int = countries.length,
                    sparkLogLevel: String = "Warn",
                    appLogLevel: String = "Info",
                    sizeThresholds: Map[String, Long] = sizeThresholds,
                    dtPattern: String = "????-??-??",
                    offset: Int = 7,
                    batchSize: Int = 7,
                    intervalSize: Int = 7,
                    dimUserTbl: String = "prod_groupondw.gbl_dim_user_uniq",
                    defaultDate: String = "9999-12-31",
                    stgOutputNumFiles: Int = 100,
                    defaultOutputNoFiles: Int = 1,
                    outNoFilesPerCountry: Map[String, Int] = outNoFilesPerCountry
                   )


  val countries = Seq("US", "CA", "BE", "FR", "DE", "IE", "IT", "NL", "PL", "ES", "AE", "UK", "JP", "AU", "NZ")

  val events: Array[String] = Array("emailDelivery", "emailSend", "emailClick", "emailBounce")

  val eventsMap: Map[String, String] = Map("emailDelivery" -> "delivery", "emailSend" -> "send",
    "emailOpenHeader" -> "open", "emailClick" -> "click", "emailBounce" -> "bounce")

  val targetLocation = "/user/grp_gdoop_edw_etl_dev/email/res"

  val sizeThresholds = Map("emailDelivery" -> 107374182L, "emailSend" -> 107374182L, "emailBounce" -> 53687091L,
    "emailOpenHeader" -> 10737418L, "emailClick" -> 10737418L)


  val tmpString: String = "_tmp"
  val stgString: String = "_stg"

  val outNoFilesPerCountry = Map("AE" -> 2, "AU" -> 4, "BE" -> 3, "CA" -> 2, "DE" -> 10, "ES" -> 10, "FR" -> 25, "IE" -> 1,
    "IT" -> 20, "JP" -> 8, "NL" -> 3, "NZ" -> 1, "PL" -> 3, "UK" -> 40, "US" -> 160)

}