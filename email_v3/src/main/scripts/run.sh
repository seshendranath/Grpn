spark-submit
--master yarn \
--deploy-mode client \
--queue dse_testing \
--driver-memory=5G \
--executor-memory=8G \
--conf spark.dynamicAllocation.maxExecutors=450 \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--conf spark.sql.shuffle.partitions=2048 \
 email.jar \
 --startTimestamp "2017-03-01 12:00:00" \
