from elasticsearch import Elasticsearch
from elasticsearch import helpers
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, date_format, to_json, lit
from pyspark.sql.avro.functions import from_avro
from confluent_kafka.schema_registry import SchemaRegistryClient
from functools import reduce

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


def flattened_df(df, batch_id):
    # Here the stream data is avro serialized
    # first 5 bits are combination of magic bits and schema id from schema repository
    # already know schema so ignoring the schema id
    value_df = (
        df
            .withColumn("value", expr("substring(value, 6, length(value)-5)"))
            .withColumn("key", expr("cast(key as string)"))
            .withColumn("batch_id", lit(str(batch_id)))
            .withColumn("timestamp", date_format("timestamp", "yyyy-MM-dd HH:mm:ss"))
    )

    deserialized_df = value_df.withColumn(
        "value",
        from_avro(col("value"), avro_schema, {"mode": "PERMISSIVE"})
    )

    # destruction the nested fields
    deflect_df = deserialized_df.selectExpr("*", "value.*").drop("value")
    transaction_fields = ["transaction_id", "user_id", "amount", "timestamp", "geo_location", "ip_address"]

    # filtering out error data
    # check if any of transaction fields are missing
    transaction_conditions = reduce(lambda acc, col_name: acc | col(col_name).isNull(), transaction_fields, lit(True))
    error_df = deflect_df.where(transaction_conditions)
    # filtering out correct data
    valid_df = deflect_df.where(~transaction_conditions)
    # checking if anomalies lies in the data
    # if the transaction amt exceed 200 then this transaction is red flag
    red_flag_df = valid_df.where("amount > 200")
    return error_df, valid_df, red_flag_df


# **************************
# Initialize Elasticsearch client
es = Elasticsearch(["http://127.0.0.1:9200"])


def write_to_elasticsearch(batch_df, es_index="transactions"):
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
        print(f"Batch failed: {e}")


# send data to file - parquet
def write_to_parquet(df):
    # write to parquet
    df.write.format("parquet").mode("append").save("./data/output/transactions")


# write data to JDBC - PostgreSQL
def write_to_postgresql(df, table="transactions"):
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
    error_df, valid_df, red_flag_df = flattened_df(df, batch_id)
    write_to_parquet(valid_df)
    write_to_elasticsearch(valid_df)
    write_to_postgresql(valid_df)

    # check if error df exist and write it to error transaction table
    if error_df.take(1):
        write_to_postgresql(error_df, table="error_transactions")
    # check if red flag transaction exist
    if red_flag_df.take(1):
        write_to_elasticsearch(red_flag_df, es_index="error_transactions")


# **************************

# Write stream to different sinks
init_df.writeStream \
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
