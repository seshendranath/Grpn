package com.groupon.edw.email

/**
  * Created by aguyyala on 2/16/17.
  */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import Ultron._
import Utils._

object Email {

  private val log = Logger.getLogger(getClass)

  def main(args: Array[String]) = {
    val emailConfig = EmailConfig.parseCmdLineArguments(args)

    val spark = getSparkSession
    val sparkContext = spark.sparkContext

    /* Setting appropriate log level */
    sparkContext.setLogLevel(emailConfig.sparkLogLevel)
    log.setLevel(Level.toLevel(emailConfig.appLogLevel))

    log.info("=" * 100 + sparkContext.applicationId + "=" * 100)

    val s = System.nanoTime()

    try {
      new EmailCore(spark, emailConfig).runner()
    }
    catch {
      case e: Exception => errorHandler(e)
    }

    def errorHandler(e: Exception) = {
      log.error("Something went WRONG during the run")
      log.error(e)
      endJob(instanceId, "failed", startTime, startTime)
    }

    val e = System.nanoTime()
    val totalTime = (e - s)/(1e9*60)
    log.info("Total Elapsed time: " + f"$totalTime%2.2f" + " mins")
    spark.stop()

  }

  def getSparkSession: SparkSession = {
    SparkSession
      .builder
      .appName("AggEmailPipeline")
      .enableHiveSupport()
      .getOrCreate()
  }

}