# -*- coding: utf-8 -*-

import numpy as np
from pymongo import MongoClient
import json
import time
import datetime
import sys
import os
import redis

from CluStream import CluStream
from MicroCluster import MicroCluster
from content2vector import http_post,http_get,http_gets
from connect_data import connect_monogo,connect_redis
from config import mongo_config_rule,redis_config_rule,similar_key

m =10
num_redis = 0
num_class =1000

def fill_attr(record, attr):
    if attr == 'write_time':
        record[attr] = [int(time.time())]
    else:
        record[attr] = '-'
    return record

def get_maxtime(write_times):
    write_times = sorted(write_times)
    maxtime = write_times[-1]
    maxtime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(maxtime))
    return maxtime

def redisoperation(r_init,r,keys,similar_key):
    key2mid = {}
    midss = []
    write_times = [] 
    key2heat = {}
    for key in keys:
        init_micro_cluster = r_init.lrange(key,0,-1)
        mids = eval(init_micro_cluster[0])
        heat = float(init_micro_cluster[1])
        init_industry = eval(init_micro_cluster[2])
        init_new_class = init_micro_cluster[3]
        times = eval(init_micro_cluster[4])
        
        r.delete(str(int(key)+num_redis))
        r.lpush(str(int(key)+num_redis),times,init_new_class,init_industry,heat,mids)
        r.sadd(similar_key['name'],str(int(key)+num_redis))

        key2mid[key] = mids
        midss += mids
        key2heat[key] = heat
        write_times += times
    return key2mid,midss,write_times,key2heat


def InitCluster(num_redis):
    #初始化
    r_init = connect_redis(redis_config_rule)

    try:    
        keys = list(r_init.smembers(similar_key['name']))
    except:
        print ("No key list:", similar_key['name'])
    print (len(keys))
    r = connect_redis(redis_config_rule)
    r.delete(similar_key['name'])

    key2mid,midss,write_times,key2heat = redisoperation(r_init,r,keys,similar_key)
    
    #get the last time for inquire in mongo 
    maxtime = get_maxtime(write_times)
    
    # mongo data extract
    my_set = connect_monogo(mongo_config_rule)
    datas = list(my_set.find({"mid": {"$in": midss}},{"mid":1,'time':1,'sort':1,'published_tag':1,'write_time':1}))
    
    datas =  [record if 'published_tag' in record else fill_attr(record,'published_tag') for record in datas]
    datas =  [record if 'sort' in record else fill_attr(record,'sort') for record in datas]
    datas =  [record if 'write_time' in record else fill_attr(record,'write_time') for record in datas]

    mid2industry = dict((record['mid'],record['sort']) for record in datas)
    mid2new_class = dict((record['mid'],record['published_tag'][0]) for record in datas)

    X_timestampe = dict((record['mid'],time.mktime(time.strptime(record['write_time'][-1], "%Y-%m-%d %H:%M:%S"))) for record in datas) 

    #i =0
    micro_clusters =[]
    for key in keys:
        mids = key2mid[key]
        try:
            nlp_vecs = http_gets(mids)
            nlp_vecs = json.loads(nlp_vecs)
            nlp_vecs = nlp_vecs['data']
        except:
            print ("nlp failed!")
            continue

        mid2nlp = dict((i,j) for i,j in zip(mids,nlp_vecs))
        length = 256
        for nlp_vec in nlp_vecs:
            if len(nlp_vec) != 1:
                length = len(nlp_vec)

        industries = {}
        new_classes = {}
        
        CFx1 = np.zeros(length)
        CFx2 = np.zeros(length)
        for mid in mids:
            nlp_vec = mid2nlp[mid]
            if nlp_vec == ['-1'] or nlp_vec == [-1]:
                continue
            
            nlp_vec = list(map(float,nlp_vec))
           
            try:
                industry = mid2industry[mid]
            except:
                industry = '-'
            try:
                new_class = mid2new_class[mid]
            except:
                new_class = '-'

            if industry in industries:
                industries[industry]+=1
            else:
                industries[industry] = 1
            
            if new_class in new_classes:
                new_classes[new_class] +=1
            else:
                new_classes[new_class] =1

            nlp_vec = np.array(nlp_vec)   
            CFx1 += nlp_vec
            CFx2 += pow(nlp_vec,2)
        heat = key2heat[key]
        micro_cluster = MicroCluster(nb_points =len(mids),identifier = int(key),\
                        linear_sum =CFx1,squared_sum = CFx2,\
                        heat = heat,industry = industries,new_classes = new_classes)
    
        micro_clusters.append(micro_cluster)
    #print len(micro_clusters)
    cluster = CluStream( nb_initial_points = 2000,time_window = 1000,\
                   nb_micro_cluster=1000,nb_created_clusters =len(micro_clusters),\
                   micro_clusters=micro_clusters,glob_radius = 0.03)
    #print maxtime

    return cluster,X_timestampe,maxtime

if __name__ == "__main__":
    InitCluster(num_redis)

