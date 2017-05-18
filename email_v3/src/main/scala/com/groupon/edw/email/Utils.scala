package com.groupon.edw.email

/**
  * Created by aguyyala on 2/16/17.
  */

import com.github.nscala_time.time.Imports._
import org.apache.hadoop.fs.Options.Rename
import org.apache.hadoop.fs.{FileContext, FileSystem, Path}
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SaveMode._
import org.apache.spark.sql._
import org.apache.hadoop.hive.metastore.api._
import org.apache.hadoop.hive.metastore.TableType

import scala.collection.mutable
import scala.collection.JavaConverters._


import Ultron._

object Utils {

  //  val yyyy_MM_dd = DateTimeFormat.forPattern("yyyy-MM-dd")

  val yyyy_MM_dd = "yyyy-MM-dd"
  val timeFormat = "yyyy-MM-dd HH:mm:ss"
  val formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")

  val sparkToHiveDataType: Map[String, String] = Map("StringType" -> "string", "LongType" -> "bigint", "IntegerType" -> "int")

  val formatInfo = Map("orc" -> Map("serde" -> "org.apache.hadoop.hive.ql.io.orc.OrcSerde",
    "inputFormat" -> "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat",
    "outputFormat" -> "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat"),
    "parquet" -> Map("serde" -> "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
      "inputFormat" -> "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
      "outputFormat" -> "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"),
    "csv" -> Map("serde" -> "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
      "inputFormat" -> "org.apache.hadoop.mapred.TextInputFormat",
      "outputFormat" -> "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"))

  val log = Logger.getLogger(getClass)
  log.setLevel(Level.toLevel("Info"))

  log.info("Fetching Data from Ultron")
  val (instanceId, startTime) = startJob()
  log.info(s"Ultron StartTime: $startTime")

  /**
    * Mapping Input/Output formats from Hive to Spark
    */
  def getSparkFileFormat(fileFormat: String): String = {
    val format = fileFormat.toLowerCase

    format match {
      case _ if format contains "parquet" => "parquet"
      case _ if format contains "orc" => "orc"
    }

  }

  /**
    * Calculate Start and End Timestamp
    */
  def getStartEndTimeStamp(startTimeStamp: Option[String], endTimeStamp: Option[String]): (DateTime, DateTime) = {

    // val startDt = if (startTimeStamp.isDefined) DateTime.parse(startTimeStamp.getOrElse("None"), formatter) else DateTime.now - 1.day
    val startDt = if (startTimeStamp.isDefined) DateTime.parse(startTimeStamp.getOrElse("None"), formatter) else DateTime.parse(startTime, formatter)
    val endDt = if (endTimeStamp.isDefined) DateTime.parse(endTimeStamp.getOrElse("None"), formatter) else DateTime.now

    (startDt, endDt)
  }

  def seqToQuotedString(seq: Seq[String]) = {
    seq.mkString("'", "', '", "'")
  }

  def getHiveTableLocation(hiveMetaStore: HiveMetaStoreClient, dbName: String, tableName: String): String = {
    hiveMetaStore.getTable(dbName, tableName).getSd.getLocation
  }

  def getHiveInputFormat(hiveMetaStore: HiveMetaStoreClient, dbName: String, tableName: String): String = {
    val hiveFormat = hiveMetaStore.getTable(dbName, tableName).getSd.getInputFormat
    getSparkFileFormat(hiveFormat)
  }

  def saveDataFrameToHdfs(df: DataFrame, path: String, format: String, partCols: Array[String]) = {
    log.info(s"Writing DataFrame to $path")
    df.write.mode(Overwrite).format(format).partitionBy(partCols: _*).save(path)
  }

  def saveDataFrameToHdfs(df: DataFrame, path: String, format: String) = {
    log.info(s"Writing DataFrame to $path")
    if (format != "csv") {
      df.write.mode(Overwrite).format(format).save(path)
    }
    else {
      df.write.mode(Overwrite).format(format).option("sep", "\u0001").save(path)
    }

  }

  def hdfsRemoveAndMove(dfs: FileSystem, dfc: FileContext, srcPath: String, tgtPath: String): Unit = {
    if (dfs.exists(new Path(tgtPath))) dfc.delete(new Path(tgtPath), true)
    dfs.mkdirs(new Path(tgtPath))
    hdfsMove(dfc, srcPath, tgtPath)
  }

  def hdfsMove(dfc: FileContext, srcPath: String, tgtPath: String): Unit = {
    log.info(s"Moving $srcPath to $tgtPath")
    dfc.rename(new Path(srcPath), new Path(tgtPath), Rename.OVERWRITE)
  }

  def constructHiveColString(df: DataFrame, excludeCol: Seq[String]): String = {
    var res = ""
    for ((col, dataType) <- df.dtypes) {
      if (!(excludeCol contains col)) res += col + " " + sparkToHiveDataType(dataType) + ","
    }
    res.stripSuffix(",")
  }

  /**
    * Checks the DDL info of the passed table and verifies it against passed info
    */
  def checkAndCreateHiveDDL(hiveMetaStore: HiveMetaStoreClient, dbName: String, tblName: String, format: String,
                            location: String, cols: Seq[(String, String)], partCols: Seq[String]) = {

    val tblExist = hiveMetaStore.tableExists(dbName, tblName)

    val ddlMatch = if (tblExist) checkDDL(hiveMetaStore, dbName, tblName, cols, partCols) else false

    if (!tblExist || !ddlMatch) {
      log.info("TABLE Does Not Exists OR DDL Mismatch, Creating New One")

      if (tblExist) hiveMetaStore.dropTable(dbName, tblName)

      val s = new SerDeInfo()
      s.setSerializationLib(formatInfo(format)("serde"))
      s.setParameters(Map("serialization.format" -> "1").asJava)

      val sd = new StorageDescriptor()
      sd.setSerdeInfo(s)
      sd.setInputFormat(formatInfo(format)("inputFormat"))
      sd.setOutputFormat(formatInfo(format)("outputFormat"))

      val tblCols = mutable.ListBuffer[List[FieldSchema]]()
      for (col <- cols) {
        val (c, dataType) = col
        tblCols += List(new FieldSchema(c, dataType, c))
      }
      sd.setCols(tblCols.toList.flatMap(x => x).asJava)

      sd.setLocation(location)

      val t = new Table()

      t.setSd(sd)

      val tblPartCols = mutable.ListBuffer[List[FieldSchema]]()
      for (col <- partCols) {
        tblPartCols += List(new FieldSchema(col, "string", col))
      }
      t.setPartitionKeys(tblPartCols.toList.flatMap(x => x).asJava)

      t.setTableType(TableType.EXTERNAL_TABLE.toString)
      t.setDbName(dbName)
      t.setTableName(tblName)
      t.setParameters(Map("EXTERNAL" -> "TRUE", "tableType" -> "EXTERNAL_TABLE").asJava)

      hiveMetaStore.createTable(t)

    }

  }

  def checkDDL(hiveMetaStore: HiveMetaStoreClient, dbName: String, tblName: String, cols: Seq[(String, String)],
               partCols: Seq[String]): Boolean = {

    val tbl = hiveMetaStore.getTable(dbName, tblName)
    val hiveCols = tbl.getSd.getCols.asScala.map(x => (x.getName, x.getType)).toArray
    val hivePartCols = tbl.getPartitionKeys.asScala.map(x => (x.getName, x.getType)).toArray
    (cols.map(x => (x._1.toLowerCase, x._2.toLowerCase)) ++ partCols.map(x => (x, "string"))).toArray.deep == (hiveCols ++ hivePartCols).deep

  }


  def addHivePartitions(hiveMetaStore: HiveMetaStoreClient, dbName: String, tblName: String, parts: List[(List[String], String)]) = {

    val table = hiveMetaStore.getTable(dbName, tblName)
    val partitions = for (part <- parts) yield {
      val (p, loc) = part
      val partition = new Partition()
      partition.setDbName(dbName)
      partition.setTableName(tblName)
      val sd = new StorageDescriptor(table.getSd)
      sd.setLocation(loc)
      partition.setSd(sd)
      partition.setValues(p.asJava)
      partition
    }
    hiveMetaStore.add_partitions(partitions.asJava, true, true)
  }

  def addHivePartition(hiveMetaStore: HiveMetaStoreClient, dbName: String, tblName: String, part: (List[String], String)) = {

    val table = hiveMetaStore.getTable(dbName, tblName)
    val (p, loc) = part
    val partition = new Partition()
    partition.setDbName(dbName)
    partition.setTableName(tblName)
    val sd = new StorageDescriptor(table.getSd)
    sd.setLocation(loc)
    partition.setSd(sd)
    partition.setValues(p.asJava)

    hiveMetaStore.add_partition(partition)
  }

}
