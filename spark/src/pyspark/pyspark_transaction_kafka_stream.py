from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
from pyspark.sql.avro.functions import from_avro
from confluent_kafka.schema_registry import SchemaRegistryClient

from config import sr_config, kafka_config

# Initialize Spark session
spark = SparkSession.builder \
    .appName("PySparkStreamingKafkaAvroIntegration") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6,"
            "org.apache.spark:spark-avro_2.12:3.5.6,"
            "io.confluent:kafka-avro-serializer:7.3.3") \
    .getOrCreate()

# Set log to Warn
spark.sparkContext.setLogLevel('ERROR')

# Kafka & Schema Registry configurations
kafka_broker = kafka_config["bootstrap.servers"]
topic = "transactions"

# Set up Schema Registry client
schema_registry_client = SchemaRegistryClient(sr_config)

# Fetch schema by subject
# subject_name = f"{topic}-value"
# schema = schema_registry_client.get_latest_version(subject_name).schema
# avro_schema = schema.schema_str
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
    .load()

avro_schema = """
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

query = deserialized_df \
    .writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
