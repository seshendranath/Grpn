/**
 * tungsten/tungsten-replicator/samples/scripts/batch/hadoop.js
 * Tungsten Scale-Out Stack
 * Copyright (C) 2013-2014 Continuent Inc.
 * Contact: tungsten@continuent.org
 *
 * Load script for HDFS using hadoop utility.  Tables load to corresponding
 * directories in HDFS.  
 * 
 * This script handles data loading from multiple replication services by 
 * ensuring that each script includes the replication service name in the 
 * HDFS directory.  The target directory format is the following: 
 * 
 *   /user/tungsten/staging/<service name>
 */

// Called once when applier goes online. 
function prepare()
{
  // Ensure target directory exists.  This must contain the service name. 
  logger.info("Executing hadoop connect script to create data directory");
  service_name = runtime.getContext().getServiceName();
  hadoop_base = "/user/tungsten/staging/" + service_name;
  hadoop_base_stg = "/user/tungsten/stg/" + service_name;
  runtime.exec('hadoop fs -mkdir -p ' + hadoop_base);
  runtime.exec('hadoop fs -mkdir -p ' + hadoop_base_stg);
}

// Called at start of batch transaction. 
function begin()
{
  // Does nothing. 
}

// Appends data from a single table into a file within an HDFS directory. 
// If the key is present that becomes a sub-directory so that data are 
// distributed. 
function apply(csvinfo)
{
  // Collect useful data. 
  csv_file = csvinfo.file.getAbsolutePath();
  schema = csvinfo.schema;
  table = csvinfo.table;
  seqno = csvinfo.startSeqno;
  key = csvinfo.key;
  if (key == "") {
    hadoop_dir = hadoop_base + '/' + schema + "/" + table;
    hadoop_dir_stg = hadoop_base_stg + '/' + schema + "/" + table;
  }
  else {
    hadoop_dir = hadoop_base + '/' + schema + "/" + table + "/" + key
    hadoop_dir_stg = hadoop_base_stg + '/' + schema + "/" + table + "/" + key
  }
  hadoop_file = hadoop_dir + '/' + table + '-' + seqno + ".csv";
  hadoop_file_stg = hadoop_dir_stg + '/' + table + '-' + seqno + ".csv";
  logger.info("Writing file: " + csv_file + " to: " + hadoop_file_stg);

  // Ensure the directory exists for the file. 
  runtime.exec('hadoop fs -mkdir -p ' + hadoop_dir);
  runtime.exec('hadoop fs -mkdir -p ' + hadoop_dir_stg);

  // Remove any previous file with this name.  That takes care of restart
  // by wiping out earlier loads of the same data. 
  runtime.exec('hadoop fs -rm -f ' + hadoop_file);
  runtime.exec('hadoop fs -rm -f ' + hadoop_file_stg);

  // Load file to HDFS. 
  runtime.exec('hadoop fs -put ' + csv_file + ' ' + hadoop_file_stg);
  
  // Moving file from STG to Main Staging
  runtime.exec('hadoop fs -mv ' + hadoop_file_stg + ' ' + hadoop_file);

}

// Called at commit time for a batch. 
function commit()
{
  // Does nothing. 
}

// Called when the applier goes offline. 
function release()
{
  // Does nothing. 
}

