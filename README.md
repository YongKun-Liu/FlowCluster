## 流式聚类

### 文件说明
* script
* * start.sh: 启动文件
* * shutdown.sh：停止文件

* tool
* * InitCluster.py: 初始化文件，用来初始化聚类中心和检测时间
* * main_new2.py: 流聚类过程，执行流聚类
* * CluStream.py: 聚类操作定义文件
* * MicroCluster.py: 聚类中心定义文件
* * connet_data.py: mongodb和redis连接函数
* * content2vector.py:内容转nlp向量函数
* * config.py:配置文件
* * log_config.py:日志配置文件
* * OfflineCluster.py: 话题聚合任务，每分钟执行一次
* * Offline2Weibo.py:热点聚合任务，每小时执行一次

### 操作执行

* 启动流聚类：sh /script/start.sh
* 关闭流聚类：sh /script/shutdown.sh


