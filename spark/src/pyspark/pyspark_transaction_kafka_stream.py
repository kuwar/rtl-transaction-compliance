from elasticsearch import Elasticsearch
from elasticsearch import helpers
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, date_format
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
topic = "transactions"

# Set up Schema Registry client
schema_registry_client = SchemaRegistryClient(sr_config)

# Fetch schema by subject
subject_name = f"{topic}-value"
schema = schema_registry_client.get_latest_version(subject_name).schema
avro_schema = schema.schema_str
print("Avro schema in confluent schema registry")
print(avro_schema)
avro_options = {
    "schema.registry.url": "http://192.168.1.17:8081/",
    "mode": "PERMISSIVE"
}

# Consume data from Kafka topic
init_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "192.168.1.17:9092,192.168.1.17:9093,192.168.1.17:9094") \
    .option("subscribe", topic) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()
"""
avro_schema = 
{
    "namespace": "financials.transaction",
    "name": "Transaction",
    "type": "record",
    "fields": [
        {"name": "transaction_id", "type": "string"},
        {"name": "user_id", "type": "string"},
        {"name": "amount", "type": "double"},
        {"name": "timestamp", "type": "string"},
        {
            "name": "geo_location",
            "type": {
                "name": "Geolocation",
                "type": "record",
                "fields": [
                    {"name": "lat", "type": "string"},
                    {"name": "lon", "type": "string"}
                ]
            }
        },
        {"name": "ip_address", "type": "string"}
    ]
}
"""

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


def send_to_elasticsearch_with_error_handling(batch_df, batch_id):
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


# **************************

# Writing to console
"""
query = deserialized_df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

"""
"""
query = deserialized_df \
    .writeStream \
    .queryName("stream_transaction_writing_to_es") \
    .outputMode("append") \
    .format("es") \
    .option("es.nodes", "127.0.0.1") \
    .option("es.port", "9200") \
    .option("es.resource", "transactions") \
    .option("es.nodes.wan.only", "true") \
    .option("checkpointLocation", "./es_transactions_checkpoint") \
    .start()

query.awaitTermination()
"""

# Write stream to Elasticsearch
transformed_df.writeStream \
    .foreachBatch(send_to_elasticsearch_with_error_handling) \
    .outputMode("append") \
    .start() \
    .awaitTermination()
