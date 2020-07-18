# -*- coding: utf-8 -*-
"""
Created on Tue Mar 12 17:19:12 2019

@author: yongkun1
"""
import numpy as np
from pymongo import MongoClient
import json
import time
import datetime
import sys
import os
import redis
import math
import argparse

from CluStream import CluStream
from MicroCluster import MicroCluster
from content2vector import http_post,http_get
from InitCluster import InitCluster 
from connect_data import connect_monogo,connect_redis
from config import mongo_config_rule,redis_config_rule,similar_key 

m =10
num_redis = 0
num_class =1000

def main():

    # init cluster centers     
    cluster1, X_timestamp, maxtime = InitCluster(num_redis)

    print ("begin")
    #处理数据流
    save_date = time.strftime("%Y-%m-%d", time.localtime())
    r = connect_redis(redis_config_rule)
    my_set = connect_monogo(mongo_config_rule)

    while True:
        if maxtime < '2019-06-09 12:00:00':
            maxtime  = '2019-06-09 12:00:00'
        print (maxtime)
        data = list(my_set.find({'write_time':{'$gt':maxtime}},\
                {"mid":1,"sort":1,"exp":1,"content":1,"write_time":1,"published_tag":1}).\
                limit(80).sort("time",1))
        for record in data:
            mid = record['mid']
            try:
                write_time = record['write_time'][-1]
            except:
                write_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())) 

            if maxtime<write_time:
                    maxtime = write_time

            write_time = datetime.datetime.strptime(write_time, "%Y-%m-%d %H:%M:%S")
            write_time  = write_time.timetuple()
            write_time = int(time.mktime(write_time))

            if mid in X_timestamp:
                continue  #跳过记录
            else:
                X_timestamp[mid] = write_time
            
            current_time = int(time.time())

            for i in list(X_timestamp.keys()):
                if current_time - int(X_timestamp[i]) > 3600*36:
                    del X_timestamp[i]

            try:
                exp = record['exp'][-1]
            except:
                exp = 0
            try:
                industry = record['sort']
            except:
                industry = '-'

            try:
                new_class = record["published_tag"][0]
            except:
                new_class = "-1"

            tmp = []
            # NLP特征 
            try:
                nlp_vec = http_get([mid])
                nlp_vec = json.loads(nlp_vec)
                nlp_vec = nlp_vec["data"][0]
                nlp_vec = list(map(float,nlp_vec))
            except:
                print ("No NLP")
                continue  #跳过记录

            if len(nlp_vec) == 1:
                #print (nlp_vec)
                continue #跳过记录

            tmp.append(nlp_vec) #初始化所用数据
            tmp.append(write_time)
            tmp.append(exp)
            tmp.append(industry)
            tmp.append(new_class)
                #print (tmp)
            x_index,Flag =  cluster1.partial_fit(tmp) #数据流输入

            if Flag: #更新类，再放入redis，添加mid，heat，industry，new_class值变动,替换times中某一个
                print True
                clusters = cluster1.micro_clusters
                for i in clusters:
                    if i.identifier == x_index:
                        update_cluster = i
                        break

                heat = update_cluster.heat
                industries = update_cluster.industry
                new_classes = update_cluster.new_classes

                old_times = eval(r.lrange(x_index+num_redis,-1,-1)[0])
                update_time = write_time
                if len(old_times)>=m+1:
                    del old_times[1]
                    old_times.append(update_time)
                else:
                    old_times.append(update_time)
                

                industries1 = update_cluster.industry_top3()
                new_class_select = update_cluster.new_classtop()

                #添加mid
                mids = eval(r.lrange(x_index+num_redis,0,0)[0])
                mids.append(mid)

                r.delete(str(x_index+num_redis))
                r.lpush(str(x_index+num_redis),old_times)
                r.lpush(str(x_index+num_redis),new_class_select)
                r.lpush(str(x_index+num_redis),industries1)
                r.lpush(str(x_index+num_redis),heat)
                r.lpush(str(x_index+num_redis),mids)

            else: #新建类
                print (False)
                r.delete(str(x_index+num_redis)) #删除旧的类别
                times = []

                creat_time = write_time

                times.append(creat_time)
                times.append(creat_time)
                r.lpush(str(x_index+num_redis),times)
                r.lpush(str(x_index+num_redis),new_class)
                r.lpush(str(x_index+num_redis),[industry])
                r.lpush(str(x_index+num_redis),exp)
                r.lpush(str(x_index+num_redis),[mid])
                r.sadd(similar_key['name'],x_index+num_redis)


if __name__ == "__main__":
    main()
