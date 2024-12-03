from pyspark.sql import SparkSession
from pyspark.sql import types as T

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
spark.sparkContext.setLogLevel("ERROR")

schema = T.StructType([
    T.StructField("name", T.StringType(), True),
    T.StructField("age", T.IntegerType(), True)
])

data = [
    ["Shaurave", 36],
    ["Samjhana", 33],
]

df = spark.createDataFrame(data, schema)

df.show(truncate=False)

# Write DataFrame to Elasticsearch

(
    df.write
        .format("org.elasticsearch.spark.sql")
        .option("es.resource", "people")
        .mode("append")
        .save()
)
