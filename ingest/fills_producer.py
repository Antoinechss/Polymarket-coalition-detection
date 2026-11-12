# ingest logs from the API and publish on a Kafka topic for stream processing

import json, time
from pathlib import Path
from kafka import KafkaProducer
from ingest.decoders.orders_filled import decode
from common.rpc import rpc
from common.params import EXCHANGES

producer = KafkaProducer(bootstrap_servers="localhost:9092", value_serializer=str.encode)

SAFETY_GAP = 5 # Stay 5 blocks behind the head to avoid reorged blocks 
SLEEP = 20
MAX_LAG = 1000
CURSOR_PATH = Path(__file__).parent / "state" / "cursor.txt"

head = int(rpc("eth_blockNumber"), 16) - SAFETY_GAP

if CURSOR_PATH.exists():
    with open(CURSOR_PATH) as c:
        cursor = int(c.read())
else: 
    cursor = head - 50

while True: 
    head = int(rpc("eth_blockNumber"), 16) - SAFETY_GAP

    lag = head - cursor
    if lag > MAX_LAG:
        skipped_from, skipped_to = cursor + 1, head - MAX_LAG
        print(f"Too many blocks behind: skipping blocks")
        cursor = head - MAX_LAG

    while cursor < head:
        to_block = min(cursor + 10, head)
        batch_logs = rpc("eth_getLogs",
                 [{"fromBlock": hex(cursor + 1),
                   "toBlock": hex(to_block),
                   "address": EXCHANGES,
        }])
        n_fills_sent = 0 
        for log in batch_logs:
            order = decode(log)
            if order is not None:
                producer.send("polygon-orders", 
                              key=order["maker"].encode(),
                              value=json.dumps(order))
                n_fills_sent += 1
        print(f"Blocks {cursor + 1} to {to_block}: sent {n_fills_sent} fills")
        producer.flush()

        cursor = to_block
        with open(CURSOR_PATH, "w") as c:
            c.write(str(cursor))
        
        
    time.sleep(SLEEP)
