package com.groupon.edw.email

/**
  * Created by aguyyala on 2/16/17.
  */


import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


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

    new EmailCore(spark, emailConfig).runner()

    spark.stop()

  }

  def getSparkSession: SparkSession = {
    SparkSession
      .builder
      .appName("AggEmailPipeline")
      .enableHiveSupport()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.dynamicAllocation.enabled", "False")
      .config("spark.sql.shuffle.partitions", 1024)
      .getOrCreate()
  }

}