import json
import os
from kafka import KafkaProducer

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