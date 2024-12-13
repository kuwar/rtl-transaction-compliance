from elasticsearch import Elasticsearch
from elasticsearch import helpers
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, date_format, to_json
from pyspark.sql.avro.functions import from_avro
from confluent_kafka.schema_registry import SchemaRegistryClient

from config import sr_config, kafka_config

# Initialize Spark session
spark = SparkSession.builder \
    .appName("PySparkStreamingKafkaAvroIntegrationWithElasticsearch") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,"
            "org.apache.spark:spark-avro_2.12:3.5.3,"
            "io.confluent:kafka-avro-serializer:7.5.0,"
            "org.elasticsearch:elasticsearch-spark-30_2.12:8.16.1") \
    .config("es.nodes", "127.0.0.1") \
    .config("es.port", "9200") \
    .config("es.nodes.wan.only", "true") \
    .getOrCreate()

# Set log to Warn
spark.sparkContext.setLogLevel("ERROR")

# Kafka & Schema Registry configurations
kafka_broker = kafka_config["bootstrap.servers"]
topic = "transactions_avro"

# Set up Schema Registry client
schema_registry_client = SchemaRegistryClient(sr_config)

# Fetch schema by subject
subject_name = f"{topic}-value"
schema = schema_registry_client.get_latest_version(subject_name).schema
avro_schema = schema.schema_str
print("Avro schema in confluent schema registry")
print(avro_schema)
avro_options = {
    "schema.registry.url": "http://localhost:8081/",
    "mode": "PERMISSIVE"
}

# Consume data from Kafka topic
init_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094") \
    .option("subscribe", topic) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

# Remove Schema Registry metadata
"""
Schema Registry Metadata: When using Confluent Schema Registry, Avro messages are prefixed with a magic byte (0) and a 4-byte schema ID. This prefix must be removed before deserialization.
Incorrect Kafka Producer Serialization: The producer may not have serialized the messages correctly using the Avro schema.
Mismatch Between Avro Schema and Serialized Data: The schema used in Spark does not match the schema used by the producer.
"""
cleaned_data = init_df.withColumn("value", expr("substring(value, 6, length(value)-5)"))

deserialized_df = cleaned_data.select(
    from_avro(col("value"), avro_schema, {"mode": "PERMISSIVE"}).alias("data")
).select("data.*")

transformed_df = deserialized_df.withColumn("timestamp", date_format("timestamp", "yyyy-MM-dd HH:mm:ss"))
# **************************
# Initialize Elasticsearch client
es = Elasticsearch(["http://127.0.0.1:9200"])


def write_to_elasticsearch(batch_df, batch_id):
    try:
        # Convert DataFrame rows to dictionaries with recursive conversion of nested Rows
        dict_list = [row.asDict(recursive=True) for row in batch_df.collect()]
        actions = [
            {
                "_index": "transactions",
                "_source": doc
            }
            for doc in dict_list
        ]
        # helpers.bulk(es, actions)
        # Use helpers.bulk with error tracking
        success, errors = helpers.bulk(es, actions, raise_on_error=False)

        # Print the failed documents
        print(f"Number of successful documents: {success}")
        print(f"Number of failed documents: {len(errors)}")
        for error in errors:
            print(error)
    except Exception as e:
        print(f"Batch {batch_id} failed: {e}")


# send data to file - parquet
def write_to_parquet(df, batch_id):
    # write to parquet
    df.write.format("parquet").mode("append").save("./data/output/transactions")


# write data to JDBC - PostgreSQL
def write_to_postgresql(df, batch_id):
    df.show()
    df.printSchema()
    df_postgresql = df.withColumn("geo_location", to_json(col("geo_location")))
    df_postgresql.printSchema()

    (
        df_postgresql
            .write
            .mode("append")
            .format("jdbc")
            .option("driver", "org.postgresql.Driver")
            .option("url", "jdbc:postgresql://localhost:5432/postgres")
            .option("dbtable", "transactions")
            .option("user", "postgres")
            .option("password", "postgres")
            .option("createTableColumnTypes", "geo_location JSON")
            .save()
    )


# manage different sink
def write_to_sink(df, batch_id):
    write_to_parquet(df, batch_id)
    write_to_elasticsearch(df, batch_id)
    write_to_postgresql(df, batch_id)


# **************************

# Write stream to different sinks
transformed_df.writeStream \
    .foreachBatch(write_to_sink) \
    .outputMode("append") \
    .start() \
    .awaitTermination()

"""
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.apache.spark:spark-avro_2.12:3.5.3,org.elasticsearch:elasticsearch-spark-30_2.12:8.16.1 \
  --jars ./jars/postgresql-42.7.2.jar \
  ./pyspark_transaction_kafka_stream.py
"""
