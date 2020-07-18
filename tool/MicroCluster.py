#coding=utf:8

import math as math
import numpy as np


class MicroCluster:
    """
    Implementation of the MicroCluster data structure for the CluStream algorithm
    Parameters
    ----------
    :parameter nb_points is the number of points in the cluster
    :parameter identifier is the identifier of the cluster (take -1 if the cluster result from merging two clusters) id
    :parameter merge is used to indicate whether the cluster is resulting from the merge of two existing ones
    :parameter id_list is the id list of merged clusters
    :parameter linear_sum is the linear sum of the points in the cluster.
    :parameter squared_sum is  the squared sum of all the points added to the cluster.
    :parameter linear_time_sum is  the linear sum of all the timestamps of points added to the cluster.
    :parameter squared_time_sum is  the squared sum of all the timestamps of points added to the cluster.
    :parameter m is the number of points considered to determine the relevance stamp of a cluster
    :parameter update_timestamp is used to indicate the last update time of the cluster
    """

    def __init__(self, nb_points=0, identifier=0, id_list=[], linear_sum=None,
                 squared_sum=None, heat=0, industry = {}, new_classes = {},
                 m=2, update_timestamp=0):
        self.nb_points = nb_points
        self.identifier = identifier
        self.id_list = id_list
        self.linear_sum = linear_sum
        self.squared_sum = squared_sum
       
       #new attributes
        self.heat = heat
        self.industry = industry
        self.new_classes = new_classes
        
        self.m = m
        self.update_timestamp = update_timestamp
        self.radius_factor = 1.8
        self.epsilon = 0.00005
        self.min_variance = math.pow(1, -5)

    #获得中心点
    def get_center(self):
        center = [self.linear_sum[i] / self.nb_points for i in range(len(self.linear_sum))]
        return center
    #获得权重
    def get_weight(self):
        return self.nb_points

    #插入
    def insert(self,new_point,current_timestamp, heat, sort,new_class,flag = True):
        self.nb_points += 1
        self.update_timestamp = current_timestamp
        if flag:  #如果flag为True，才参与聚类中心计算
            for i in range(len(new_point)):
                #print (new_point[i])
                self.linear_sum[i] += new_point[i]
                self.squared_sum[i] += math.pow(new_point[i], 2)

        self.heat += heat
        if sort in self.industry:
            self.industry[sort] +=1
        else:
            self.industry[sort] = 1

        if new_class in self.new_classes:
            self.new_classes[new_class] +=1
        else:
            self.new_classes[new_class] = 1


    def get_relevancestamp(self):
        if (self.nb_points < 2 * self.m):
            return self.get_mutime()
        #print (self.get_mutime())
        #print (self.get_sigmatime())
        return self.get_mutime() + self.get_sigmatime() * self.get_quantile(self.m /(2 * self.nb_points))

    #平均时间
    def get_mutime(self):
        return self.linear_time_sum / self.nb_points
    #
    def get_sigmatime(self):
        
        #print (self.squared_time_sum / self.nb_points)
        #print (math.pow((self.linear_time_sum / self.nb_points), 2))

        return math.sqrt(abs(self.squared_time_sum / self.nb_points - math.pow((self.linear_time_sum / self.nb_points), 2)))

    def get_quantile(self, x):
        assert(x >= 0 and x <= 1)
        return math.sqrt(2) * self.inverse_error(2 * x - 1)
    #获得半径
    def get_radius(self):
        if self.nb_points == 1:
            return 0
        return self.get_deviation() * self.radius_factor

    def get_clsuter_feature(self):
        return self.this
    #偏差
    def get_deviation(self):
        variance = self.get_variance_vec()
        sum_deviation = 0
        for i in range(len(variance)):
            sqrt_deviation = math.sqrt(variance[i])
            sum_deviation += sqrt_deviation
        return sum_deviation / len(variance)

    def get_variance_vec(self):
        variance_vec = list()
        for i in range(len(self.linear_sum)):
            ls_mean = self.linear_sum[i] / self.nb_points
            ss_mean = self.squared_sum[i] / self.nb_points
            variance = ss_mean - math.pow(ls_mean, 2)
            if variance <= 0:
                if variance > - self.epsilon:
                    variance = self.min_variance

            variance_vec.append(variance)
        return variance_vec

    def inverse_error(self, x):
        z = (math.sqrt(math.pi) * x)
        inv_error = z / 2
        z_prod = math.pow(z,3)
        inv_error += (1 / 24) * z_prod

        z_prod *= math.pow(z,2)
        inv_error += (7 / 960) * z_prod

        z_prod = math.pow(z,2)
        inv_error += (127 * z_prod) / 80640

        z_prod = math.pow(z,2)
        inv_error += (4369 / z_prod) * 11612160

        z_prod = math.pow(z,2)
        inv_error += (34807 / z_prod) * 364953600

        z_prod = math.pow(z,2)
        inv_error += (20036983 / z_prod) * 0x797058662400d
        return z_prod
    def industry_top3(self):
        industries = sorted(self.industry.items(),key=lambda item:item[1],reverse =True)
        num_industry = 0
        for industry  in industries:
            num_industry+=industry[1]
        industries1 = []
        score =0
        for i in range(3):
            score  += industries[i][1]/float(num_industry)
            industries1.append(industries[i][0])
            if score >= 0.8:
                break
        return industries1
    
    def new_classtop(self):
        new_classes = sorted(self.new_classes.items(),key=lambda item:item[1],reverse =True)
        return new_classes[0][0]

        
