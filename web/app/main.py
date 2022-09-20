import json
import os
from typing import List

from dataclasses import dataclass

from kafka import KafkaProducer

from cassandra.cluster import Cluster

from sonyflake import SonyFlake

from fastapi import FastAPI
from pydantic import BaseModel

from pymemcache.client.base import Client

app = FastAPI()

@dataclass
class Review():
    review_id: int
    title: str
    content: str
    rating: int

@dataclass
class Reviews():
    reviews: List[Review]

class ReviewRequest(BaseModel):
    title: str
    content: str
    rating: int

@app.get("/v1")
def read_root():
    return {"Hello": "World"}

@app.get("/v1/{company_id}/reviews")
def read_item(company_id: int) -> Reviews:
    with getCassandraCluster().connect('reviews') as session:
        memcachedClient = getMemcachedClient()
        company_version_key = f"company_id_{company_id}_version"
        version = memcachedClient.get(company_version_key)
        if not version:
            _, version = session.execute(
                        """
                        SELECT company_id, MAX(writetime(content)) as version
                        FROM reviews
                        WHERE company_id = %s
                        """,(company_id,)
                    ).one()
            memcachedClient.set(company_version_key, version)
        else:
            print(f"latest version in cache is {version}")

        rows = session.execute("""
            SELECT review_id, title, content, rating
            FROM reviews
            WHERE company_id = %s""", (company_id,))
        reviews = []
        for review_id, title, content, rating in rows:
            reviews.append(Review(review_id, title, content, rating))
        return Reviews(reviews)

@app.post("/v1/{company_id}/reviews")
def post_item(company_id: int, request: ReviewRequest) -> Review:
    idGenerator = getIdGenerator()
    id = idGenerator.next_id()
    message =  {"company_id": company_id, "review_id": id,
        "title": request.title, "content":request.content, "rating": request.rating}
    future = getKafkaProducer().send('reviews', message, key=company_id)
    future.get(timeout=60)
    
    review = Review(id, request.title, request.content, request.rating)
    print(review)
    return review

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

cassandraCluster = None
def getCassandraCluster() -> Cluster:
    global cassandraCluster
    if cassandraCluster:
        return cassandraCluster

    cassandra_server = os.environ['CASSANDRA_SERVER']
    print(f"cassandra connecting to {cassandra_server}")
    cassandraCluster = Cluster([cassandra_server])
    return cassandraCluster

memcachedClient = None
def getMemcachedClient() -> Client:
    global memcachedClient
    if memcachedClient:
        return memcachedClient

    memcached_server = os.environ['MEMCACHED_SERVER']
    print(f"memcache client connecting to {memcached_server}")
    memcachedClient = Client(memcached_server)
    return memcachedClient