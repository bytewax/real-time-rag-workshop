import json
import os
from dataclasses import dataclass, field
from datetime import timedelta
from typing import List, Dict

import websockets
from bytewax import operators as op
from bytewax.connectors.files import FileSink
from bytewax.dataflow import Dataflow
from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition, batch_async
from bytewax.connectors.kafka import operators as kop
from bytewax.connectors.kafka import KafkaSinkMessage

API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")
ticker_list = ["*"]

BROKERS = os.getenv("BROKER")
OUT_TOPIC = os.getenv("TOPIC_NEWS")

async def news_aggregator(ticker):
    url = "wss://stream.data.alpaca.markets/v1beta1/news"
    async with websockets.connect(url) as websocket:
        await websocket.send(json.dumps({"action": "auth", "key": API_KEY, "secret": API_SECRET}))
        await websocket.recv()  # Ignore auth response
        await websocket.send(json.dumps({"action": "subscribe", "news": [ticker]}))
        await websocket.recv()  # Ignore subscription response
        await websocket.recv()

        while True:
            message = await websocket.recv()
            articles = json.loads(message)
            yield articles

class NewsPartition(StatefulSourcePartition):
    def __init__(self, ticker):
        self.ticker = ticker
        self.batcher = batch_async(news_aggregator(ticker), timedelta(seconds=0.5), 100)

    def next_batch(self):
        return next(self.batcher)

    def snapshot(self):
        return None  # Stateless for now

    def close(self):
        pass  # The async context manager will handle closing the WebSocket.

@dataclass
class NewsSource(FixedPartitionedSource):
    tickers: List[str] = field(default_factory=lambda: ["*"])

    def list_parts(self):
        return self.tickers

    def build_part(self, step_id, for_key, _resume_state):
        return NewsPartition(for_key)

def process_article(state, article):
    source, news = article
    return (source, news['headline'])  # Simplified processing

flow = Dataflow("news_loader")
inp = op.input("news_input", flow, NewsSource(ticker_list)).then(op.flat_map, "flatten", lambda x: x)
op.inspect("input", inp)

def serialize_k(news)-> KafkaSinkMessage[Dict, Dict]:
    return KafkaSinkMessage(
        key=json.dumps(news['symbols'][0]),
        value=json.dumps(news),
    )

def serialize(news):
    return (news['symbols'][0], json.dumps(news))

serialized = op.map("serialize", inp, serialize)
op.output("output", serialized, FileSink('news_out.jsonl'))

# print(f"Connecting to brokers at: {BROKERS}, Topic: {OUT_TOPIC}")

# serialized = op.map("serialize", inp, serialize_k)

# broker_config = {
#     "security_protocol":"SASL_SSL",
#     "sasl_mechanism":"SCRAM-SHA-256",
#     "sasl_plain_username":"demo",
#     "sasl_plain_password":"Qq2EnlzHpzv3RZDAMjZzfZCwrFZyhK"
#     }
# kop.output("out1", serialized, brokers=BROKERS, topic=OUT_TOPIC, )