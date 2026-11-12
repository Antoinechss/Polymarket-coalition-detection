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

# --- Build buy-side features: what an informed buyer looks like
is_buy = F.col("side") == "BUY"

features = (
    fills.groupBy("token_id", "maker")
    .agg(
        F.sum(F.when(is_buy, F.col("usdc"))).alias("buy_usdc"),        # money put into buys
        F.sum(F.when(is_buy, F.col("shares"))).alias("buy_shares"),    # shares bought
        F.count(F.when(is_buy, 1)).alias("n_buys"),                    # number of buys
        F.sum(F.when(is_buy, F.col("shares"))
               .otherwise(-F.col("shares"))).alias("net_shares"),     # bought - sold = position held
    )
    .filter(F.col("n_buys") > 0).filter(F.col("net_shares") > 0)                                    
    .withColumn("avg_buy_usdc", F.col("buy_usdc") / F.col("n_buys"))         # average buy size
    .withColumn("avg_buy_price", F.col("buy_usdc") / F.col("buy_shares"))    # low = longshot conviction
    .withColumn("potential_profit", F.col("net_shares") - F.col("buy_usdc")) 
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