SET hive.auto.convert.join=true;
SET mapred.job.queue.name=${queue};
SET hive.exec.compress.output=true;
SET hive.execution.engine=tez;
SET hive.tez.container.size=8192;
SET tez.queue.name=${queue};
SET tez.runtime.io.sort.mb=2040;
SET hive.tez.java.opts=-Xmx6553m;
SET mapreduce.map.memory.mb=8192;
SET mapreduce.map.java.opts=-Xmx6553m;
SET mapreduce.reduce.memory.mb=8192;
SET mapreduce.reduce.java.opts=-Xmx6553m;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.dynamic.partition=true;
SET hive.exec.max.dynamic.partitions.pernode=1000;
SET hive.exec.max.dynamic.partitions=10000;

USE ${tgtDb};

ALTER TABLE {tgtTbl} DROP IF EXISTS PARTITION (event_date > '0', country > '0') PURGE;

INSERT OVERWRITE TABLE {tgtTbl} partition(event_date, country)
SELECT
        {cols},
        eventDate AS event_date,
        country
FROM ${srcTbl}
WHERE eventDate = '${eventDate}' and platform='email' and eventDestination='{eventDestination}' and event='{event}' and country in ("US", "CA", "BE", "FR", "DE", "IE", "IT", "NL", "PL", "ES", "AE", "UK", "JP", "AU", "NZ");
