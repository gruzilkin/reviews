import os
from cassandra.cluster import Cluster

cassandraCluster = None

def getCassandraCluster() -> Cluster:
    global cassandraCluster
    if cassandraCluster:
        return cassandraCluster

    cassandra_server = os.environ['CASSANDRA_SERVER']
    print(f"cassandra connecting to {cassandra_server}")
    cassandraCluster = Cluster([cassandra_server])
    return cassandraCluster