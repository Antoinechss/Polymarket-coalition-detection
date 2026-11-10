# ingest logs from the API and publish on a Kafka topic for stream processing

import json, time
from kafka import KafkaProducer
from chain_decoder import decode, rpc
from params import EXCHANGES

producer = KafkaProducer(bootstrap_servers="localhost:9092", value_serializer=str.encode)

cursor = int(rpc("eth_blockNumber"), 16)

SAFETY_GAP = 5 # Stay 5 blocks behind the head to avoid reorged blocks 
SLEEP = 20

while True: 
    head = int(rpc("eth_blockNumber"), 16) - SAFETY_GAP
    while cursor < head:
        to_block = min(cursor + 10, head)
        batch_logs = rpc("eth_getLogs",
                 [{"fromBlock": hex(cursor + 1),
                   "toBlock": hex(to_block),
                   "address": EXCHANGES,
        }])
        for log in batch_logs:
            order = decode(log)
            if order is not None:
                producer.send("polygon-orders", json.dumps(order))
        cursor = to_block
    producer.flush()
    time.sleep(SLEEP)
