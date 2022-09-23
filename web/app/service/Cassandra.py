import os
from cassandra.cluster import Cluster

cassandraCluster = None

def getCassandraCluster() -> Cluster:
    global cassandraCluster
    if cassandraCluster:
        return cassandraCluster

    cassandra_servers = os.environ['CASSANDRA_SERVERS']
    cassandraCluster = Cluster([server.strip() for server in cassandra_servers.split(",")])
    return cassandraCluster