import pyspark 
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType , DoubleType 
from pyspark.sql.functions import col, from_json

# --- Load Kafka package compatible with Spark version 
KAFKA_PKG = f"org.apache.spark:spark-sql-kafka-0-10_2.13:{pyspark.__version__}"

# --- Initialize spark session
spark = (SparkSession.builder
         .appName("polymarket-coalition")
         .config("spark.jars.packages", KAFKA_PKG)
         .getOrCreate())

spark.sparkContext.setLogLevel("WARN") # Avoid printing every log 

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

fills = (raw.select(from_json(col("value").cast("string"),         # Bytes -> JSON text
                       FILL_SCHEMA).alias("f"))                    # JSON text -> struct column f
                       .select("f.*"))                             # Unpacks structure f into column 
fills = (fills.withColumn("event_time", col("ts").cast("timestamp")) # Convert LongType timestamp into actual timestamp
                         .filter(col("event_time").isNotNull()))


# --- Display in console 
query = (fills.writeStream.format("console") 
            .option("truncate", False)
            .start())
query.awaitTermination()