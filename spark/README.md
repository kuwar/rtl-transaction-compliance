### install python dependencies
```python
pip install requests confluent-kafka fastavro 
```

### Run the scripts
spark-submit \
    --master spark://192.168.1.17:7077 \
    --deploy-mode client \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-avro_2.12:3.5.0 \
    ./pyspark_transaction_kafka_stream.py

### get IP of running container
docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' spark-master

eg. 172.18.0.2

/opt/spark/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    ./example.py

docker exec -it spark-master /opt/bitnami/spark/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    ./example.py

docker cp ./example.py spark-master:/opt/spark/example.py

spark-submit \
    --verbose \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-avro_2.12:3.5.0,io.confluent:kafka-avro-serializer:7.5.0 \
    ./pyspark_transaction_kafka_stream.py


spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.apache.spark:spark-avro_2.12:3.5.3,io.confluent:kafka-avro-serializer:7.5.0,org.elasticsearch:elasticsearch-spark-30_2.12:8.16.1 \
    ./pyspark_transaction_kafka_stream.py

spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.apache.spark:spark-avro_2.12:3.5.3,org.elasticsearch:elasticsearch-spark-30_2.12:8.16.1 \
    ./pyspark_transaction_kafka_stream.py


spark-submit \
    --packages org.elasticsearch:elasticsearch-spark-30_2.12:8.16.1 \
    ./spark_elastic_data_load_demo.py

