import os
import platform
from pathlib import Path


if platform.system() == "Windows":
    java_home = os.environ.get("JAVA_HOME")
    if not java_home or not Path(java_home, "bin", "java.exe").is_file():
        java_candidates = sorted(Path("C:/Program Files/Java").glob("jdk*/bin/java.exe"))
        if java_candidates:
            os.environ["JAVA_HOME"] = str(java_candidates[-1].parent.parent)

    hadoop_home = os.environ.get("HADOOP_HOME")
    if not hadoop_home or not Path(hadoop_home, "bin", "winutils.exe").is_file():
        raise RuntimeError(
            "Windows Spark requires winutils.exe. Set HADOOP_HOME to a Hadoop "
            "directory containing bin\\winutils.exe before running this script."
        )


from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    from_utc_timestamp,
    sum,
    when,
    window,
)
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "user-events"

CHECKPOINT_LOCATION = "spark/checkpoints/features_parquet"
OUTPUT_LOCATION = "spark/output/features"


spark = (
    SparkSession.builder
    .appName("StreamRec-StreamingFeatures")
    .master("local[2]")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.ui.enabled", "false")
    .config(
        "spark.hadoop.fs.file.impl",
        "org.apache.hadoop.fs.RawLocalFileSystem",
    )
    .config("spark.hadoop.io.native.lib.available", "false")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0",
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# Read events from Kafka

raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)


# Define the event schema

event_schema = StructType([
    StructField("event_id", StringType()),
    StructField("user_id", IntegerType()),
    StructField("item_id", IntegerType()),
    StructField("event_type", StringType()),
    StructField("event_time", StringType()),
    StructField("device", StringType()),
    StructField("country", StringType()),
    StructField("session_id", StringType()),
])


# Decode Kafka JSON

events = (
    raw_stream
    .select(
        from_json(
            col("value").cast("string"),
            event_schema,
        ).alias("event")
    )
    .select("event.*")
)


# Convert event_time into a Spark timestamp

events = events.withColumn(
    "event_time",
    col("event_time").cast("timestamp"),
)


# Watermark + 5-minute sliding window

windowed = (
    events
    .withWatermark(
        "event_time",
        "30 seconds",
    )
    .groupBy(
        col("user_id"),
        window(
            col("event_time"),
            "5 minutes",
            "1 minute",
        ),
    )
    .agg(
        sum(
            when(col("event_type") == "view", 1)
            .otherwise(0)
        ).alias("views_5m"),

        sum(
            when(col("event_type") == "click", 1)
            .otherwise(0)
        ).alias("clicks_5m"),

        sum(
            when(col("event_type") == "search", 1)
            .otherwise(0)
        ).alias("searches_5m"),

        sum(
            when(col("event_type") == "add_to_cart", 1)
            .otherwise(0)
        ).alias("add_to_carts_5m"),

        sum(
            when(col("event_type") == "purchase", 1)
            .otherwise(0)
        ).alias("purchases_5m"),
    )
)


# Write features to Parquet

query = (
    windowed
    .writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", OUTPUT_LOCATION)
    .option(
        "checkpointLocation",
        CHECKPOINT_LOCATION,
    )
    .trigger(processingTime="30 seconds")
    .start()
)


query.awaitTermination()