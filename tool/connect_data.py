# -*- coding: utf-8 -*-

import numpy as np
from pymongo import MongoClient
import json
import time
import datetime
import sys
import os
import redis


def connect_monogo(mongo_config):
    try:
    #if True:
        conn = MongoClient(mongo_config['host'],mongo_config['port'],connect=False) #入库
        db = conn.hotspot
        db.authenticate(mongo_config['user'], mongo_config['passwd'])
        coll = mongo_config['db']
        my_set = db[coll]
        return my_set
    except:
        print ("failed to connect mongo")
        return None

def connect_redis(redis_config):
    try:
        pool = redis.ConnectionPool(host = redis_config['host'],port = redis_config['port'],decode_responses =True)
        r = redis.Redis(connection_pool = pool)
        return r
    except:
        print ("failed to connect redis")
        return None

