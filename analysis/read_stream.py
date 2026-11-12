from pyspark.sql import SparkSession
from common.spark_fills import KAFKA_PKG, read_fills

# --- Initialize spark session
spark = (SparkSession.builder
         .appName("polymarket-coalition")
         .config("spark.jars.packages", KAFKA_PKG)
         .getOrCreate())

spark.sparkContext.setLogLevel("WARN") # Avoid printing every log

fills = read_fills(spark)

# --- Display in console
query = (fills.writeStream.format("console")
            .option("truncate", False)
            .start())
query.awaitTermination()
