from pyspark.sql import SparkSession
from pyspark.sql import types as T

# Initialize spark session
spark = (
    SparkSession
        .builder
        .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-30_2.12:8.16.1")
        .config("es.nodes", "127.0.0.1:9200")
        .config("es.index.auto.create", "true")
        .config("es.nodes.wan.only", "true")
        .appName("SparkToElastic")
        .getOrCreate()
)
# Set log level
spark.sparkContext.setLogLevel("ERROR")

# Schema of data
schema = T.StructType([
    T.StructField("name", T.StringType(), True),
    T.StructField("age", T.IntegerType(), True)
])

# Sample data
data = [
    ["John", 36],
    ["Doe", 33],
]
# Create Spark DataFrame
df = spark.createDataFrame(data, schema)

# Write DataFrame to Elasticsearch
(
    df.write
        .format("org.elasticsearch.spark.sql")
        .option("es.resource", "people")
        .mode("append")
        .save()
)
