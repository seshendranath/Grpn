//Loading Data from Mysql to Cassandra using Spark

/**
SAMPLE CONFIG File

target = {
    table = orders
    keyspace = groupon_production
}

source = {
    url = "jdbc:mysql://orders-db8.snc1:3306/groupon_production"
    driver = com.mysql.jdbc.Driver
    dbtable = orders
    partitionColumn = id
    numPartitions = 20000
    fetchSize = 100000
    username = <>
    password = <>
}

spark.cassandra.connection.host = "pit-cs3-uat,pit-cs2-uat"
 */


package com.groupon.edw

import java.io.File

import com.typesafe.config.{ConfigRenderOptions, Config, ConfigFactory}
import org.apache.spark.sql.{SaveMode, SQLContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, SparkConf}


/**
 * Created by aguyyala on 4/22/16.
 */
 
object Database {


  def main(args: Array[String]) = {
    var sc: SparkContext = null
    val reference = ConfigFactory.parseResources(this.getClass.getClassLoader, "defaults.conf")
    val inputConf =  ConfigFactory.parseFile(new File(args(0)))
    val conf = inputConf withFallback reference
    println(conf.root().render(ConfigRenderOptions.concise()))
    try {
      sc = new SparkContext(sparkConf(conf))
      val sqlcontext = new org.apache.spark.sql.SQLContext(sc)
      val dataFrame = sqlDataFrame(sqlcontext, conf)
      dataFrame.persist(StorageLevel.MEMORY_AND_DISK_SER)
      
      var outputWritten = 0L
      var inputRecords = 0L

	  sc.addSparkListener(new SparkListener() {
  		override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
    		val metrics = taskEnd.taskMetrics
    		if(metrics.inputMetrics != None){
    		inputRecords += metrics.inputMetrics.get.recordsRead}
    		if(metrics.outputMetrics != None){
    			outputWritten += metrics.outputMetrics.get.recordsWritten } } })
    
	  dataFrame.save("org.apache.spark.sql.cassandra", SaveMode.Overwrite, options = Map(
    	    "table" -> conf.getString("target.table"), "keyspace" -> conf.getString("target.keyspace")))

      println("outputWritten",outputWritten)

    } catch {
      case e: Throwable => {
        println(e.getMessage)
        throw e
      }
    }
    finally {
      if (sc != null)
        sc.stop()
    }
  }

  def sqlDataFrame(sqlContext: SQLContext, config: Config) = {

    val mysql = new MySQL(s"${config.getString("source.url")}?user=${config.getString("source.username")}&password=${config.getString("source.password")}")
    val (lowerBound, upperBound) =  mysql.getPartitionKeyBounds(config.getString("source.partitionColumn"),config.getString("source.dbtable"))

      sqlContext.read.format("jdbc")
      .option("url", config.getString("source.url"))
      .option("driver", config.getString("source.driver"))
      .option("dbtable", config.getString("source.dbtable"))
      .option("partitionColumn",config.getString("source.partitionColumn"))
      .option("lowerBound", lowerBound.toString)
      .option("upperBound", upperBound.toString)
      .option("numPartitions",config.getString("source.numPartitions"))
      .option("fetchSize", config.getString("source.fetchSize"))
      .option("user", config.getString("source.username"))
      .option("password", config.getString("source.password")).load()
  }


  def sparkConf(config: Config) = {

    new SparkConf()
      .set("spark.cassandra.connection.host", config.getString("spark.cassandra.connection.host"))
      .set("spark.driver.allowMultipleContexts", config.getString("spark.driver.allowMultipleContexts"))
      .set("spark.executor.memory", config.getString("spark.executor.memory"))
      .setAppName("CassandraLoader")

  }


}