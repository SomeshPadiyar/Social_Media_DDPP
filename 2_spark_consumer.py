import os
import sys
import subprocess
import pyspark

# ==========================================
# 0. AUTOMATIC JAVA 17 CONFIGURATION
# ==========================================
try:
    java_17_path = subprocess.check_output(["/usr/libexec/java_home", "-v", "17"]).decode("utf-8").strip()
    os.environ["JAVA_HOME"] = java_17_path
    print(f"✅ PySpark using Java 17: {java_17_path}")
except Exception as e:
    print("🚨 Fatal Error: Could not find Java 17.")
    sys.exit(1)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from delta import configure_spark_with_delta_pip

# ==========================================
# 1. LOCAL KAFKA SETTINGS
# ==========================================
BOOTSTRAP_SERVER = 'localhost:9092' 
KAFKA_TOPIC = 'youtube_stream'

# ==========================================
# 2. SPARK INITIALIZATION (SMART VERSIONING)
# ==========================================
spark_version = pyspark.__version__

# Spark 4.x dropped Scala 2.12 support, so we dynamically set the correct one
scala_version = "2.13" if spark_version.startswith("4") else "2.12"
kafka_package = f"org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}"

print(f"🔄 Detected PySpark v{spark_version}. Requesting Scala {scala_version} Kafka Package...")

builder = SparkSession.builder \
    .appName("YouTubeLocalStreaming") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder, extra_packages=[kafka_package]).getOrCreate()
spark.sparkContext.setLogLevel("WARN") 

# ==========================================
# 3. SCHEMA & STREAMING
# ==========================================
schema = StructType([
    StructField("video_id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("category_id", StringType(), True),
    StructField("views", IntegerType(), True),
    StructField("likes", IntegerType(), True),
    StructField("comments", IntegerType(), True),
    StructField("timestamp", DoubleType(), True)
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVER) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("processed_at", current_timestamp())

print("\n⏳ Spark Consumer Initialized. Waiting for local data...\n")

query = parsed_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "./checkpoints/youtube") \
    .start("./delta_youtube_table")

query.awaitTermination()