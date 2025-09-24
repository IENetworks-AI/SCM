from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from kafka import KafkaProducer, KafkaConsumer
from typing import List
import json
import os

# ---------------- Config ----------------
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")  # Docker network hostname
DEFAULT_TOPIC = os.getenv("KAFKA_TOPIC", "scm_topic")

app = FastAPI(title="Kafka API", version="1.0")

# ---------------- Producer Setup ----------------
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# ---------------- Request Models ----------------
class ProduceMessage(BaseModel):
    topic: str = DEFAULT_TOPIC
    key: str | None = None
    value: dict

class ConsumeRequest(BaseModel):
    topic: str = DEFAULT_TOPIC
    group_id: str = "scm_api_group"
    auto_offset_reset: str = "latest"  # or "earliest"
    limit: int = 10  # how many messages to return

# ---------------- Routes ----------------
@app.get("/health")
async def health():
    return {"status": "ok", "broker": KAFKA_BROKER, "default_topic": DEFAULT_TOPIC}


@app.post("/produce")
async def produce_message(msg: ProduceMessage):
    try:
        future = producer.send(
            msg.topic,
            key=msg.key.encode("utf-8") if msg.key else None,
            value=msg.value
        )
        future.get(timeout=10)
        return {"status": "success", "topic": msg.topic, "value": msg.value}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/consume")
async def consume_messages(req: ConsumeRequest):
    try:
        consumer = KafkaConsumer(
            req.topic,
            bootstrap_servers=[KAFKA_BROKER],
            group_id=req.group_id,
            auto_offset_reset=req.auto_offset_reset,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            enable_auto_commit=True,
            consumer_timeout_ms=5000
        )

        messages: List[dict] = []
        for i, msg in enumerate(consumer):
            if i >= req.limit:
                break
            messages.append({
                "topic": msg.topic,
                "partition": msg.partition,
                "offset": msg.offset,
                "key": msg.key.decode("utf-8") if msg.key else None,
                "value": msg.value
            })

        consumer.close()
        return {"count": len(messages), "messages": messages}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
