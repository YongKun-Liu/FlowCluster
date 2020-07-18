#coding=utf:8

from sklearn.base import BaseEstimator, ClusterMixin
from sklearn.utils import check_array
from sklearn.cluster import KMeans
from MicroCluster import MicroCluster as model
from scipy.spatial import distance
import math
import numpy as np
import threading
import time
import sys
from connect_data import connect_redis
from config import redis_config_rule,similar_key

num_redis = 0


class CluStream(BaseEstimator, ClusterMixin):
    #Implementation of CluStream

    def __init__(self, nb_initial_points=2000, nb_created_clusters= 0,\
            time_window=10,\
            timestamp=0, \
            clocktime=0,\
            nb_micro_cluster=200,\
            nb_macro_cluster=5,\
            micro_clusters=[],\
            h=1000,\
            glob_radius = 0.03 ):   #glob_radius:阈值半径

        self.start_time = 0
        self.nb_initial_points = nb_initial_points
        self.time_window = time_window  # Range of the window
        self.timestamp = timestamp
        self.clocktime = clocktime
        self.micro_clusters = micro_clusters
        self.nb_micro_cluster = nb_micro_cluster
        self.nb_macro_cluster = nb_macro_cluster
        self.h = h
        #self.snapshots = []
        self.nb_created_clusters = nb_created_clusters
        
        self.glob_radius = glob_radius
        
    #初始化时也要考虑阈值半径，目前先使用原始初始化方法
    def fit1(self, X, Y=None):
        X = np.array(X)
        X_vec = X[:,0]  #提取出特征
        X_kmeans =[]
        for i in X_vec:
            X_kmeans.append(i)
        X_kmeans = check_array(X_kmeans, accept_sparse='csr')  #返回一个二维有效数组
        nb_initial_points = X.shape[0]
        if nb_initial_points >= self.nb_initial_points:
            kmeans = KMeans(n_clusters=self.nb_micro_cluster, random_state=1)
            labels = kmeans.fit_predict(X_kmeans, Y)
            cluster_centers = kmeans.cluster_centers_

            X = np.column_stack((labels, X))  #合并
            select_index = []
            labels1 = labels.copy()
        
            for i in range(len(X)):  #删除大于阈值的
                if self.distance_to_cluster(cluster_centers[X[i][0]],X[i][1]) < self.glob_radius:
                    select_index.append(i)
                else:
                    labels1[i] = -1

            X1 = X[select_index]
            initial_clusters =[]
            for i in range(self.nb_micro_cluster):
                #print (i)
                if X1[X1[:,0]==i] is not None:
                    clusters = X1[X1[:,0]==i][:,1:]
                else:
                    clusters =[]
                initial_clusters.append(clusters)
            #print (len(initial_clusters))

            #initial_clusters = [X1[X1[:, 0] == l][:, 1:] for l in set(labels1) if l != -1]
            labels = np.array(labels)
            res = set(labels[select_index])
            lab = set(labels)
            out = lab -res
            for l in out:
                if l != -1:
                    index = np.where(labels == l)
                    index1 = index[0][0]
                    #print index1
                    initial_clusters[l] =[X[index1][1:]]
                    labels1[index1] = l
                    #print (micro_cluster_labels[index1])
                    continue
            for cluster in initial_clusters:
                if cluster is None:
                    print ("Creat")
                #print cluster
                clus_id = self.create_micro_cluster(cluster)
        self.start_time = time.time()
        #print (set(labels1))
        return labels1

    def create_micro_cluster(self, cluster,old_identifier= None):
        linear_sum = np.zeros(len(cluster[0][0]))
        squared_sum = np.zeros(len(cluster[0][0]))
        if old_identifier != None:
            new_m_cluster = model(identifier=old_identifier, nb_points=0, linear_sum=linear_sum,squared_sum=squared_sum, update_timestamp=0,heat= 0,industry ={},new_classes = {} )
        else:
            new_m_cluster = model(identifier=self.nb_created_clusters, nb_points=0, linear_sum=linear_sum,squared_sum=squared_sum, update_timestamp=0,heat = 0, industry ={},new_classes = {} )
            self.nb_created_clusters += 1
        for point in cluster:
            new_m_cluster.insert(point[0], point[1], point[2], point[3],point[4])
            #print 'new_m_cluster:',new_m_cluster.nb_points

        self.micro_clusters.append(new_m_cluster)
        #print (len(new_m_cluster.mids))
        return self.nb_created_clusters-1
        


    #计算x到聚类中心的距离
    def distance_to_cluster(self, x, cluster):
        if not isinstance(cluster, list):
            return distance.cosine(x, cluster.get_center())
        else:
            return distance.cosine(x, cluster)

	#找到最近的聚类中心,和距离
    def find_closest_cluster(self, x, micro_clusters):
        min_distance = sys.float_info.max
        for cluster in micro_clusters:
            distance_cluster = self.distance_to_cluster(x, cluster)
            if distance_cluster < min_distance:
                min_distance = distance_cluster
                closest_cluster = cluster
        return closest_cluster, abs(min_distance)
    
    def check_fit_in_cluster(self, x, cluster,min_distance):
        nums = cluster.nb_points  #(50/np.sqrt(n))
        if nums<1:
            nums = 1
        factor = (5/np.sqrt(nums))
        #factor = min(factor,1)
        if self.glob_radius*factor < min_distance:  #如果大于阈值直接作为新类
            if 0.15 < min_distance:
                return False, False
            else:
                return True, False
        else:
            return True, True


    #找到最旧的更新聚类
    def oldest_updated_cluster(self):
        threshold = self.timestamp - self.time_window
        min_relevance_stamp = sys.float_info.max
        oldest_cluster = None
        for cluster in self.micro_clusters:
            relevance_stamp = cluster.get_relevancestamp()
            if  relevance_stamp < min_relevance_stamp:
                min_relevance_stamp = relevance_stamp
                oldest_cluster = cluster
        return oldest_cluster


    def oldest_updated_cluster1(self):
        r = connect_redis(redis_config_rule)
        keys = r.smembers(similar_key['name'])
        key2time = {}
        for key in keys:
            times = eval(r.lrange(key,-1,-1)[0])
            avg_time = sum(times[1:])/(len(times)-1)
            key2time[key] = avg_time
        sort_key2time = sorted(key2time.items(),key=lambda item:item[1],reverse =False)

        smal_time = int(sort_key2time[0][1])
        smal_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(smal_time))
        #print smal_time
        #print int(sort_key2time[0][0]) - num_redis
        old_cluster = None
        for cluster in self.micro_clusters:
            if int(cluster.identifier) == int(sort_key2time[0][0]) - num_redis:
                old_cluster =  cluster
                break
        #print old_cluster
        return old_cluster

    def partial_fit(self, x):
        X = x[0]
        #print ("closet")
        closest_cluster, min_distance = self.find_closest_cluster(X, self.micro_clusters)  #找到最近的距离和中心
        check,flag = self.check_fit_in_cluster(X, closest_cluster, min_distance)  #判断
        if check:  #如果属于原来的类
            closest_cluster.insert(x[0],x[1],x[2],x[3],x[4],flag)
            return closest_cluster.identifier,True
        else:
            if len(self.micro_clusters)<1000:
                #print len(self.micro_clusters)
                new_identifier = self.create_micro_cluster([x])
                #print new_identifier
                return new_identifier,False
            else:
                old_up_clust = self.oldest_updated_cluster1()
                if old_up_clust is not None:
                    old_identifier = old_up_clust.identifier
                    self.micro_clusters.remove(old_up_clust)  #删除
                    x = [x]
                    self.create_micro_cluster(x,old_identifier)
                return old_identifier,False

    

    
    
