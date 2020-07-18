# -*- coding: utf-8 -*-
__author__ = 'yongkun1'
import logging
import logging.handlers

import sys

import os

class Config():

    def __init__(self):
        self.path = "../py_log/"
        self.filename = self.path + "log"

        #创建一个logger
        self.logger = logging.getLogger('Cluster2Hive')
        self.logger.setLevel(logging.DEBUG)

        #定义handler的输出格式
        self.formatter = logging.Formatter('%(message)s')


        #创建一个handelr，用于写入日志文件
        self.timefile_handler = logging.handlers.TimedRotatingFileHandler(self.filename,when = "MIDNIGHT",interval =1,backupCount=3) #按天分割日志文件，方便查找
        self.timefile_handler.suffix = "%Y%m%d"
        self.timefile_handler.setFormatter(self.formatter)
    
        # 再创建一个handler，用于输出到控制台
        #self.console_handler = logging.StreamHandler()
        #self.console_handler.setLevel(logging.DEBUG)
        #self.console_handler.setFormatter(self.formatter)


        #给logger添加handler
        self.logger.addHandler(self.timefile_handler)
        #self.logger.addHandler(self.console_handler)
    def getLog(self):
        return self.logger



