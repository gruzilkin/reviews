import os
from pymemcache.client.base import Client
from pymemcache import serde

def getMemcachedClient() -> Client:
    memcached_server = os.environ['MEMCACHED_SERVER']
    return Client(memcached_server, serde=serde.pickle_serde)