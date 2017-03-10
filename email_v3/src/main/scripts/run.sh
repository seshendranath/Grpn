spark-submit --master yarn \
--deploy-mode client \
--queue dse_testing \
--driver-memory=5G \
--num-executors=100 \
--executor-cores=3 \
--executor-memory=13G \
--conf spark.dynamicAllocation.enabled=false \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--conf spark.sql.shuffle.partitions=600 \
 email.jar  --startTimestamp "2017-03-01 12:00:00"