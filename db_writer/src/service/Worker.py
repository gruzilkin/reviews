import os
import json
from kafka import KafkaConsumer
from cassandra.cluster import Cluster
from pymemcache.client.base import Client
from pymemcache import serde

class Worker:
    def __init__(self) -> None:
        pass        

    def __enter__(self):
        kafka_servers = os.environ['KAFKA_SERVERS']
        self.kafka = KafkaConsumer(bootstrap_servers=kafka_servers,
                group_id='reviews_writer',
                key_deserializer=lambda k: int(bytes(k).decode('utf-8')),
                value_deserializer=lambda v: json.loads(bytes(v).decode('utf-8')))
        self.kafka.subscribe(['reviews'])

        cassandra_server = os.environ['CASSANDRA_SERVER']
        self.cassandra = Cluster([cassandra_server]).connect('reviews')
        
        memcached_server = os.environ['MEMCACHED_SERVER']
        self.memcached = Client(memcached_server, serde=serde.pickle_serde)
        return self
  
    def __exit__(self, exc_type, exc_value, traceback):
        self.kafka.close()
        self.cassandra.shutdown()
        self.memcached.close()

    def doWork(self):
        while True:
            for msg in self.kafka:
                print(msg.value)
                company_id = msg.value['company_id']
                review_id = msg.value['review_id']
                title = msg.value['title']
                content = msg.value['content']
                rating = msg.value['rating']

                self.cassandra.execute(
                    """
                    INSERT INTO reviews (company_id, review_id, title, content, rating)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (company_id, review_id, title, content, rating)
                )
                _, version = self.cassandra.execute(
                    """
                    SELECT company_id, MAX(writetime(content)) as version
                    FROM reviews
                    WHERE company_id = %s
                    """,(company_id,)
                ).one()

                self.updateCompanyDataVersion(company_id, version)

                print(f"latest version is {version}")
                print(msg)

    def updateCompanyDataVersion(self, company_id: int, new_version: int):
        key = f"company_id_{company_id}_version"
        version, cas = self.memcached.gets(key)
        if version:
            if new_version > version:
                self.memcached.cas(key, new_version, cas)
        else:
            self.memcached.add(key, new_version)
            self.updateCompanyDataVersion(company_id, new_version)
