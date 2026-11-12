from kafka import KafkaConsumer
import json, math
from anomaly_detection.models import HalfSpaceTree

model = HalfSpaceTree() # Model choice


def to_features(row: dict): 
    """Subselection of features (excl maker and token id)"""
    return {
        "total_usdc": math.log1p(row["total_usdc"]),
        "total_shares": math.log1p(row["total_shares"]),
        "n_trades": math.log1p(row["n_trades"]),
        "avg_usdc": math.log1p(row["avg_usdc"]),
        "buy_ratio": row.get("buy_ratio") if row.get("buy_ratio") is not None else 0.5,
        "net_shares": math.copysign(math.log1p(abs(row["net_shares"])), row["net_shares"])
    }

consumer = KafkaConsumer("features", bootstrap_servers="localhost:9092",
                         auto_offset_reset="earliest",
                         value_deserializer=lambda b: json.loads(b))

latest = {}   # maps (token_id, maker) → latest score
i = 0
WARMUP = 1000

for msg in consumer: 
    row = msg.value
    x = to_features(row)
    score = model.score_one(x)
    model.learn_one(x)
    i += 1
    if i < WARMUP: 
        continue

    latest[(row["token_id"], row["maker"])] = score
    
    # display top 10 scores every 500 msg 
    if i % 500 == 0: 
        top10 = sorted(latest.items(), key=lambda kv: kv[1], reverse=True)[:10]
        for (token, maker), s in top10:
            print(f"{s:.3f}  {maker}  {token[:12]}…")
    i += 1


