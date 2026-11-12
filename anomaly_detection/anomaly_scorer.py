from kafka import KafkaConsumer
import json, math
from river import anomaly, compose, preprocessing

consumer = KafkaConsumer("features", bootstrap_servers="localhost:9092",
                         auto_offset_reset="earliest",
                         value_deserializer=lambda b: json.loads(b))

hst_model = compose.Pipeline(
    preprocessing.MinMaxScaler(),
    anomaly.HalfSpaceTrees(n_trees=25, height=8, window_size=250, seed=42))

for msg in consumer: 
    
