# hadoop启动和关闭命令

```start-all.sh```  
启动所有的Hadoop守护进程。包括NameNode、 Secondary NameNode、DataNode、JobTracker、 TaskTrack  

```stop-all.sh```  
停止所有的Hadoop守护进程。包括NameNode、 Secondary NameNode、DataNode、JobTracker、 TaskTrack  

```start-dfs.sh```  
启动Hadoop HDFS守护进程NameNode、SecondaryNameNode和DataNode  

```stop-dfs.sh```  
停止Hadoop HDFS守护进程NameNode、SecondaryNameNode和DataNode  

```hadoop-daemons.sh start namenode```  
单独启动NameNode守护进程  

```hadoop-daemons.sh stop namenode```  
单独停止NameNode守护进程   

```hadoop-daemons.sh start datanode```  
单独启动DataNode守护进程  

```hadoop-daemons.sh stop datanode```  
单独停止DataNode守护进程  

```hadoop-daemons.sh start secondarynamenode```  
单独启动SecondaryNameNode守护进程  

```hadoop-daemons.sh stop secondarynamenode```  
单独停止SecondaryNameNode守护进程  

``` hadoop-daemon.sh start zkfc```
启动ZKFC  

```start-mapred.sh```  
启动Hadoop MapReduce守护进程JobTracker和TaskTracker  

```stop-mapred.sh```  
停止Hadoop MapReduce守护进程JobTracker和TaskTracker  

```hadoop-daemons.sh start jobtracker```  
单独启动JobTracker守护进程  

```hadoop-daemons.sh stop jobtracker```  
单独停止JobTracker守护进程  

```hadoop-daemons.sh start tasktracker```  
单独启动TaskTracker守护进程  

```hadoop-daemons.sh stop tasktracker```  
单独启动TaskTracker守护进程  

```start-yarn.sh```  
启动yarn，注意以上命令只能启动NodeManager，ResourceManager要用以下命令，在每台RM上单独执行启动ResourceManager进程  

```yarn-daemon.sh start resourcemanager```  
启动yarn的resourcemanager  

```less /opt/bigdata/hadoop-2.10.0/logs/hadoop-root-namenode-hadoop-01.log```  
查看namenode的log，datanode同理  

```hadoop jar hadoop-mapreduce-examples-2.10.0.jar wordcount /data/wc/input /data/wc/output```  
把hadoop的例子程序跑起来。这里要先执行```cd $HADOOP_HOME/share/hadoop/mapreduce```  