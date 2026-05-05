from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Task 3 - Filter Expensive Orders
# Goal: Display only orders where price is greater than 500.
# Question: Which orders appeared? Why did the Mouse and Keyboard orders disappear?
# Answer: Mouse (25) and Keyboard (75) have prices below 500 so they are filtered out.
#         Only Laptop (1200), Phone (800), Laptop (1100), and Phone (650) pass the filter.

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

# Task 3: keep only orders with price > 500
expensive_df = orders_df.filter(col("price") > 500)

query = expensive_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
