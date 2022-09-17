import json
import uuid
import os

from kafka import KafkaProducer

from fastapi import FastAPI

app = FastAPI()

@app.get("/v1")
def read_root():
    return {"Hello": "World"}


@app.get("/v1/{company_id}/reviews")
def read_item(company_id: int):
    return {"company_id": company_id}

@app.post("/v1/{company_id}/reviews")
def post_item(company_id: int):
    id = uuid.uuid4()
    message =  {"company_id": company_id, "review_id": id }
    future = getKafkaProducer().send('reviews', str(message), key=company_id)
    record = future.get(timeout=60)
    print(record)
    return message

kafka = None
def getKafkaProducer() -> KafkaProducer:
    global kafka

    if kafka:
        return kafka
    
    servers = os.environ['KAFKA_SERVERS']
    print(f"kafka producer connecting to {servers}")
    kafka = KafkaProducer(bootstrap_servers=servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: str(k).encode('utf-8'))
    return kafka
