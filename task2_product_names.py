from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Task 2 - Select Product Names Only
# Goal: Modify the Spark code to display only the product column.

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

# Task 2: select only the product column
products_df = orders_df.select("product")

query = products_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
