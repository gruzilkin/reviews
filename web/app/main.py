import json
import uuid
import os

from kafka import KafkaProducer

from sonyflake import SonyFlake

from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()

class Review(BaseModel):
    title: str
    content: str
    rating: int

@app.get("/v1")
def read_root():
    return {"Hello": "World"}

@app.get("/v1/{company_id}/reviews")
def read_item(company_id: int):
    return {"company_id": company_id}

@app.post("/v1/{company_id}/reviews")
def post_item(company_id: int, review: Review):
    idGenerator = getIdGenerator()
    id = idGenerator.next_id()
    message =  {"company_id": company_id, "review_id": id,
        "title": review.title, "content":review.content, "rating": review.rating}
    future = getKafkaProducer().send('reviews', message, key=company_id)
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

idGenerator = None
def getIdGenerator() -> SonyFlake:
    global idGenerator

    if idGenerator:
        return idGenerator

    idGenerator = SonyFlake()
    return idGenerator