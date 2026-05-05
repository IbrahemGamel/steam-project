from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Bonus Task - Count Orders per Customer
# Goal: Count how many orders each customer placed.
# Why complete mode? Aggregation requires the full updated result table to be
# printed each time new data arrives, so outputMode must be "complete".

spark = SparkSession.builder \
    .appName("KafkaSparkOrdersWorkshop") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("customer", StringType(), True),
    StructField("product",  StringType(), True),
    StructField("price",    IntegerType(), True),
])

raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "orders") \
    .option("startingOffsets", "latest") \
    .load()

json_df = raw_df.selectExpr("CAST(value AS STRING) as json_value")

orders_df = json_df.select(from_json(col("json_value"), schema).alias("data")) \
    .select("data.*")

# Bonus: count orders grouped by customer
count_df = orders_df.groupBy("customer").count()

query = count_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
