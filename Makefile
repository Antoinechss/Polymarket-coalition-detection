docker start kafka                               # 1. broker
python -m ingest.fills_producer                  # 2. chain → polygon-orders
python -m anomaly_detection.spark_features       # 3. polygon-orders → features
python -m anomaly_detection.outlier_scorer       # 4. features → scores