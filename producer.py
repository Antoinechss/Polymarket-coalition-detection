# ingest logs from the API and publish on a Kafka topic for stream processing

import json, time
from pathlib import Path
from kafka import KafkaProducer
from chain_decoder import decode, rpc
from params import EXCHANGES

producer = KafkaProducer(bootstrap_servers="localhost:9092", value_serializer=str.encode)

SAFETY_GAP = 5 # Stay 5 blocks behind the head to avoid reorged blocks 
SLEEP = 20

head = int(rpc("eth_blockNumber"), 16) - SAFETY_GAP

if Path("cursor.txt").exists(): 
    with open("cursor.txt") as c: 
        cursor = int(c.read())
else: 
    cursor = head - 50

while True: 
    head = int(rpc("eth_blockNumber"), 16) - SAFETY_GAP
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
        with open("cursor.txt", "w") as c: 
            c.write(str(cursor))
        
        
    time.sleep(SLEEP)
