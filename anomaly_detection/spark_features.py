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
).filter(F.col("n_trades") > 1)

# --- Display in console
query = (
    features.writeStream.outputMode("update")
    .format("console")
    .option("truncate", False)
    .start()
)
query.awaitTermination()
