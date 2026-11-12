# Shared Spark entry point: read fills from Kafka -> parse -> dedup

import pyspark
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType , DoubleType
from pyspark.sql.functions import col, from_json

# --- Kafka package compatible with Spark version (pass to spark.jars.packages when building the session)
KAFKA_PKG = f"org.apache.spark:spark-sql-kafka-0-10_2.13:{pyspark.__version__}"

# --- How late a duplicate can arrive (in event time) and still be dropped
DEDUP_WATERMARK = "10 minutes"

# --- Build fixed SCHEMA architecture and format (key, value) messages accordingly
FILL_SCHEMA = StructType([
    StructField("ts", LongType()),
    StructField("tx_hash", StringType()),
    StructField("log_index", IntegerType()),
    StructField("block", LongType()),
    StructField("order_hash", StringType()),
    StructField("maker", StringType()),
    StructField("taker", StringType()),
    StructField("side", StringType()),
    StructField("token_id", StringType()),
    StructField("shares", DoubleType()),
    StructField("usdc", DoubleType()),
    StructField("price", DoubleType()),
    StructField("fee", DoubleType()),
])


def read_fills(spark: SparkSession) -> DataFrame:
    """Streaming DataFrame of parsed, deduplicated fills from the polygon-orders topic"""

    # --- Build raw streaming dataframe from Kafka topic read
    raw = (spark.readStream
           .format("kafka")
           .option("kafka.bootstrap.servers", "localhost:9092")     # Link dedicated port
           .option("subscribe", "polygon-orders")                   # Subscribe to the topic
           .option("startingOffsets", "earliest")                   # Replay what is already in the topic
           .load()
           )

    """ Raw output format from raw.printSchema():
     |-- key: binary
     |-- value: binary
     |-- topic: string
     |-- partition: integer
     |-- offset: long
     |-- timestamp: timestamp
    """

    fills = (raw.select(from_json(col("value").cast("string"),         # Bytes -> JSON text
                           FILL_SCHEMA).alias("f"))                    # JSON text -> struct column f
                           .select("f.*"))                             # Unpacks structure f into column
    fills = (fills.withColumn("event_time", col("ts").cast("timestamp")) # Convert LongType timestamp into actual timestamp
                             .filter(col("event_time").isNotNull()))

    # --- Dedup: (tx_hash, log_index) identifies one on-chain log; the producer re-sends a batch if it dies before saving its cursor
    fills = (fills.withWatermark("event_time", DEDUP_WATERMARK)
                  .dropDuplicatesWithinWatermark(["tx_hash", "log_index"]))

    return fills
