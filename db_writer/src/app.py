import os
import json
from kafka import KafkaConsumer

def main():
    servers = os.environ['KAFKA_SERVERS']
    print(f"kafka consumer connecting to {servers}")
    
    kafka = KafkaConsumer(bootstrap_servers=servers,
        group_id='reviews_writer',
        key_deserializer=lambda k: int(bytes(k).decode('utf-8')),
        value_deserializer=lambda v: json.loads(bytes(v).decode('utf-8')))
    kafka.subscribe(['reviews'])

    try:
        while True:
            for msg in kafka:
                print(msg)
    except:
        print("finished reading consimer")
        kafka.close()

if __name__ == '__main__':
    main()
