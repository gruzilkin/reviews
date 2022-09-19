import os
import json
from kafka import KafkaConsumer
from cassandra.cluster import Cluster

def main():
    kafka_servers = os.environ['KAFKA_SERVERS']
    print(f"kafka consumer connecting to {kafka_servers}")
    
    kafka = KafkaConsumer(bootstrap_servers=kafka_servers,
        group_id='reviews_writer',
        key_deserializer=lambda k: int(bytes(k).decode('utf-8')),
        value_deserializer=lambda v: json.loads(bytes(v).decode('utf-8')))
    kafka.subscribe(['reviews'])

    cassandra_server = os.environ['CASSANDRA_SERVER']
    print(f"cassandra connecting to {cassandra_server}")
    cluster = Cluster([cassandra_server])
    session = cluster.connect('reviews')


    try:
        while True:
            for msg in kafka:
                print(msg.value)
                session.execute(
                    """
                    INSERT INTO reviews (company_id, review_id, title, content, rating)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (msg.value['company_id'], msg.value['review_id'], \
                        msg.value['title'], msg.value['content'], msg.value['rating'])
                )
                print(msg)
    finally:
        if kafka:
            kafka.close()

if __name__ == '__main__':
    main()
