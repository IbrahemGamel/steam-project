from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Task 4 - Add Discounted Price
# Goal: Add a new column called discounted_price after applying a 10% discount.
# Example: Laptop with price 1200 -> discounted_price = 1080.0

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

# Task 4: add a discounted_price column (10% off)
discount_df = orders_df.withColumn("discounted_price", col("price") * 0.9)

query = discount_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
