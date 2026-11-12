from pyspark.sql import SparkSession
from common.spark_fills import KAFKA_PKG, read_fills
from pyspark.sql import functions as F

# --- Initialize spark session
spark = (
    SparkSession.builder.appName("polymarket-coalition")
    .config("spark.jars.packages", KAFKA_PKG)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

fills = read_fills(spark)

# --- Build features for online anomaly detection ML model
features = fills.groupBy("token_id", "maker").agg(
    F.sum("usdc").alias("total_usdc"),
    F.sum("shares").alias("total_shares"),
    F.count("*").alias("n_trades"),
    F.avg("usdc").alias("avg_usdc"),
    (
        F.sum(F.when(F.col("side") == "BUY", F.col("usdc")).otherwise(0))
        / F.sum("usdc")
    ).alias(
        "buy_ratio"
    ),  # share of a wallet’s trades on this outcome that are buys rather than sells
    F.sum(
        F.when(F.col("side") == "BUY", F.col("shares")).otherwise(-F.col("shares"))
    ).alias(
        "net_shares"
    ),  # bought shares - sold shares
)

# --- Send features to Kafka topic "features"
query = (features
    .select(F.col("token_id").alias("key"),
            F.to_json(F.struct("*")).alias("value"))     # row → JSON text, the reverse of from_json
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "features")
    .option("checkpointLocation", "anomaly_detection/state/features_ckpt")   # Spark's own cursor.txt
    .outputMode("update")
    .start())

query.awaitTermination()