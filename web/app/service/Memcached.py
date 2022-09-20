import os
from pymemcache.client.base import Client

memcachedClient = None

def getMemcachedClient() -> Client:
    global memcachedClient
    if memcachedClient:
        return memcachedClient

    memcached_server = os.environ['MEMCACHED_SERVER']
    print(f"memcache client connecting to {memcached_server}")
    memcachedClient = Client(memcached_server)
    return memcachedClient