from kafka import KafkaConsumer
import json, math
from river import anomaly
from anomaly_detection.models import HalfSpaceTree
import statistics

model = HalfSpaceTree() # Model choice
detector = anomaly.QuantileFilter(model, q=0.999)

def to_features(row: dict):
    """Buy-side features of one (outcome, wallet) pair, scaled for the model"""
    net = row["net_shares"]
    pp = row["potential_profit"]
    return {
        "buy_usdc":      math.log1p(row["buy_usdc"]),
        "n_buys":        math.log1p(row["n_buys"]),
        "avg_buy_usdc":  math.log1p(row["avg_buy_usdc"]),
        "net_shares":    math.copysign(math.log1p(abs(net)), net),   # can be < 0 if it sold more than it bought
        "avg_buy_price": row["avg_buy_price"],                       # already in [0, 1]: no log
        "potential_profit" :math.copysign(math.log1p(abs(pp)), pp)
    }

consumer = KafkaConsumer("features", bootstrap_servers="localhost:9092",
                         auto_offset_reset="earliest",
                         value_deserializer=lambda b: json.loads(b))

latest = {}    # (token_id, maker) → latest score (debug only)
alerts = {}    # (token_id, maker) → (score, row)
i = 0
WARMUP = 1000
PRINT_EVERY = 500
DEBUG = False

for msg in consumer:
    row = msg.value
    key = (row["token_id"], row["maker"])
    x = to_features(row)

    # 1. score before learning, 2. decide, 3. learn
    score = detector.score_one(x)
    is_alert = detector.classify(score)
    detector.learn_one(x)          # QuantileFilter skips learning on flagged points
    i += 1

    if i < WARMUP:                 # model + quantile still settling: don't trust scores
        continue

    latest[key] = score
    if is_alert:
        alerts[key] = (score, row)

    if i % PRINT_EVERY == 0:
        print(f"\n--- {i} msgs | {len(latest)} pairs scored | {len(alerts)} flagged ---")
        top = sorted(alerts.items(), key=lambda kv: kv[1][0], reverse=True)[:10]
        for (token, maker), (s, r) in top:
            print(f"{s:.3f}  {maker[:10]}…  token {token[:10]}…  "
                f"${r['buy_usdc']:>10,.0f} bought  {r['n_buys']:>5} buys  "
                f"@ {r['avg_buy_price']:.3f}  holds {r['net_shares']:>10,.0f} shares  "
                f" potential profit $ {r['potential_profit']:.3f} ")

        if DEBUG:
            scores = list(latest.values())
            q = statistics.quantiles(scores, n=100)
            print(f"p50={q[49]:.3f}  p90={q[89]:.3f}  p99={q[98]:.3f}")

