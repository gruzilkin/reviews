import os
import json
from kafka import KafkaConsumer
from cassandra.cluster import Cluster

def main():
    cassandra_server = os.environ['CASSANDRA_SERVER']
    print(f"cassandra connecting to {cassandra_server}")
    cluster = Cluster([cassandra_server])
    with cluster.connect('reviews') as session:
        kafka_servers = os.environ['KAFKA_SERVERS']
        print(f"kafka consumer connecting to {kafka_servers}")
        
        kafka = KafkaConsumer(bootstrap_servers=kafka_servers,
            group_id='reviews_writer',
            key_deserializer=lambda k: int(bytes(k).decode('utf-8')),
            value_deserializer=lambda v: json.loads(bytes(v).decode('utf-8')))
        kafka.subscribe(['reviews'])

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
                    _, version = session.execute(
                        """
                        SELECT company_id, MAX(writetime(content)) as version
                        FROM reviews
                        WHERE company_id = %s
                        """,(msg.value['company_id'],)
                    ).one()

                    print(f"latest version is {version}")
                    print(msg)
        finally:
            if kafka:
                kafka.close()

if __name__ == '__main__':
    main()
