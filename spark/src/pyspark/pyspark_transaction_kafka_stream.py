from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
from confluent_kafka.schema_registry import SchemaRegistryClient

from config import sr_config, kafka_config

# Initialize Spark session
spark = SparkSession.builder \
    .appName("PySparkStreamingKafkaAvroIntegration") \
    .getOrCreate()

# Set log to Warn
spark.sparkContext.setLogLevel('ERROR')

# Kafka & Schema Registry configurations
kafka_broker = kafka_config["bootstrap.servers"]
topic = "transactions"

# Set up Schema Registry client
schema_registry_client = SchemaRegistryClient(sr_config)

# Fetch schema by subject
subject_name = f"{topic}-value"
schema = schema_registry_client.get_latest_version(subject_name).schema
avro_schema = schema.schema_str

# Consume data from Kafka topic
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_broker) \
    .option("subscribe", topic) \
    .load()

df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").writeStream \
    .outputMode("append") \
    .option("truncate", "false") \
    .format("console") \
    .start() \
    .awaitTermination()
