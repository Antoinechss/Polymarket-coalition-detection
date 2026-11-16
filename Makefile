# --- Docker container setup 
open -a Docker                     # wait for the whale icon to stop animating
cd ~/Polymarket-coalition-detection/Polymarket-coalition-detection
source polymarket-coalition-venv/bin/activate
lsof -i :9092                      # must be empty (no Homebrew Kafka)
docker start kafka

# --- Pipeline stages 
python -m ingest.fills_producer              # chain → polygon-orders
python -m anomaly_detection.spark_features   # polygon-orders → features
python -m anomaly_detection.anomaly_scorer   # features → flagged wallets

# --- Check topics 
# fills 
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --topic polygon-orders --bootstrap-server localhost:9092 --max-messages 3
# features 
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --topic features --bootstrap-server localhost:9092 --max-messages 3

# --- For structured streaming visualization 
# Spark UI: http://localhost:4040 → Structured Streaming

# --- Reset after feature change
rm -rf anomaly_detection/state/features_ckpt
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --delete --topic features --bootstrap-server localhost:9092