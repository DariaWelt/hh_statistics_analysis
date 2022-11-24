#!/bin/bash

pip install kafka-python

# start SPARK_APP via spark-submit
/opt/spark/bin/spark-submit \
--conf "spark.mongodb.read.connection.uri=$DB_URL/$DB_NAME.$DB_COLLECTION" \
--conf "spark.mongodb.write.connection.uri=$DB_URL/$DB_NAME.$DB_COLLECTION" \
--packages org.mongodb.spark:mongo-spark-connector:10.0.4 \
$SPARK_APP 

# it's crutch to prevent container stoping, remove it if you need
echo "-------------HANDLER ENDED-------------"
tail -f /dev/null