#!/bin/bash
sudo su - hadoop

#create hive db
beeline -n hadoop -u jdbc:hive2://localhost:10000 -e "DROP DATABASE KYLIN_USB_MQB CASCADE;"
beeline -n hadoop -u jdbc:hive2://localhost:10000 -e "CREATE DATABASE KYLIN_USB_MQB;"
beeline -n hadoop -u jdbc:hive2://localhost:10000 -f /home/hadoop/KYLIN_USB/sources/run/create_partitioned_tables.hql

#Deploy Hbase site
sudo cp /home/hadoop/KYLIN_USB/sources/install/hbase-site.xml /etc/hbase/conf/hbase-site.xml
#Deploy core-site.xml
sudo cp /home/hadoop/KYLIN_USB/sources/install/core-site.xml /etc/hadoop/conf/core-site.xml

#create environment variable for kylin
export KYLIN_HOME=/opt/kylin/apache-kylin-2.6.0-bin
#create local directory for kylin
sudo mkdir /opt/kylin
#change owner of the directory
sudo chown hadoop /opt/kylin

#symbolic link in hadoop conf directory
#ln -s /etc/hive/conf/hive-site.xml /etc/hadoop/conf/

#HBase EMR IP
hbase_priv_ip=$(aws ec2 describe-instances --region eu-west-1 --filters "Name=instance-state-name,Values=running" "Name=tag:app,Values=kylin-usb-hbase" "Name=tag:aws:elasticmapreduce:instance-group-role,Values=MASTER" --query "Reservations[].Instances[].[PrivateIpAddress, Tags[?Key=='Name'].Value|[0]]" --output text | awk '{print $1}')

#Moving to local directory of kylin
cd /opt/kylin

#copying archive
aws s3 cp s3://pos-install-kylin/sources/sources/apache-kylin-2.6.0-bin-hbase1x.tar.gz /opt/kylin/apache-kylin-2.6.0-bin-hbase1x.tar.gz

#unzip kylin archive
tar -xvzf 'apache-kylin-2.6.0-bin-hbase1x.tar.gz'

#Getting the line where we find the regex 'hbase.zookeeper.forum', 31 here
line=$(grep -n 'hbase.zookeeper.quorum' /etc/hbase/conf/hbase-site.xml|awk -F ":" '{print $1}')
#Getting the first line of the quorum property
lineM1=$((line -1))
#Getting the last line of the quorum property
lineP2=$((line + 2))

#Getting the number of lines and the path and writing it in temp.txt
wc -l $KYLIN_HOME/conf/kylin_job_conf.xml > temp.txt
#Getting only the number of lines of kylin_job_conf.xml
valeur=$(cut -f 1 -d ' ' temp.txt)
#Getting two lines before the last line of kylin_job_conf.xml
valeurFinale=$((valeur-2))
#removing temp.txt
rm temp.txt


#Getting the lines from kylin_job_conf.xml on which we want to add the hbase.zookeeper.quorum
head -n $valeurFinale $KYLIN_HOME/conf/kylin_job_conf.xml >> temp.txt
#Adding the hbase.zookeeper.quorum into the temp.txt
sed -n $lineM1,$lineP2\p /etc/hbase/conf/hbase-site.xml >> temp.txt
#Adding properties about compression and mapreduce temp directories
echo "
<property>
    <name>mapreduce.map.output.compress.codec</name>
    <value>org.apache.hadoop.io.compress.SnappyCodec</value>
    <description>The compression codec to use for map outputs
    </description>
</property>

<property>
    <name>mapreduce.output.fileoutputformat.compress.codec</name>
    <value>org.apache.hadoop.io.compress.SnappyCodec</value>
    <description>The compression codec to use for job outputs
    </description>
</property>

<property>
  <name>mapreduce.map.memory.mb</name>
  <value>6144</value>
  <description></description>
</property>

<property>
    <name>mapreduce.map.java.opts</name>
    <value>-Xmx5632m -XX:OnOutOfMemoryError='kill -9 %p'</value>
    <description></description>
</property>

<property>
    <name>mapreduce.task.io.sort.mb</name>
    <value>1024</value>
    <description></description>
</property>

<property>
  <name>mapreduce.reduce.memory.mb</name>
  <value>12288</value>
</property>

<property>
    <name>mapreduce.reduce.java.opts</name>
    <value>-Xmx11264m -XX:OnOutOfMemoryError='kill -9 %p'</value>
    <description></description>
</property>

<property>
  <name>mapred.local.dir</name>
  <value>/mnt/mapred</value>
</property>
<property>
    <name>mapreduce.cluster.local.dir</name>
    <value>/mnt/mapred/local</value>
</property>

<property>
    <name>mapreduce.jobtracker.system.dir</name>
    <value>/mnt/mapred/system</value>
</property>

<property>
    <name>mapreduce.jobtracker.staging.root.dir</name>
    <value>/mnt/mapred/staging</value>
</property>

<property>
   <name>mapreduce.cluster.temp.dir</name>
   <value>/mnt/mapred/temp</value>
</property>
</configuration>
" >> temp.txt

#Replacing the original kylin_job_conf.xml by the new one we have constructed through temp.txt
sudo cp -f temp.txt $KYLIN_HOME/conf/kylin_job_conf.xml

#Creating kylin directory and permissions
hadoop fs -mkdir /kylin
hadoop fs -mkdir /kylin/spark-history
hadoop fs -chmod -R 777 /kylin

#Adding these properties to kylin.properties
echo "
############################DECATHLON##########################PRE-PRODUCTION CONFIGURATION
## While hive client uses above settings to read hive table metadata
## table operations can go through a separate SparkSQL command line, given SparkSQL connects to the same Hive metastore.
#Hive Parameters
kylin.source.hive.enable-sparksql-for-table-ops=true
kylin.source.hive.sparksql-beeline-shell=/usr/lib/spark/bin/beeline
kylin.source.hive.sparksql-beeline-params=-n root --hiveconf hive.security.authorization.sqlstd.confwhitelist.append='mapreduce.job.*|dfs.*' -u jdbc:hive2://localhost:10000

#Query Pushdown parameters
kylin.query.pushdown.jdbc.driver=org.apache.hive.jdbc.HiveDriver
kylin.query.pushdown.jdbc.username=hive
kylin.query.pushdown.jdbc.password=
kylin.query.pushdown.jdbc.pool-max-total=8
kylin.query.pushdown.jdbc.pool-max-idle=8
kylin.query.pushdown.jdbc.pool-min-idle=0

#Enable web dashboard
kylin.web.dashboard-enabled=true

#Cube planer
kylin.query.metrics.enabled=true
kylin.cube.cubeplanner.enabled=true
kylin.server.query-metrics2-enabled=true
kylin.metrics.reporter-query-enabled=true
kylin.metrics.reporter-job-enabled=true
kylin.metrics.monitor-enabled=true

#Kylin algorithm
kylin.cube.algorithm=layer

#Hbase & HDFS parameters
kylin.storage.hbase.coprocessor-mem-gb=20
kylin.storage.hbase.namespace=KYLIN_USB
kylin.storage.hbase.cluster-fs=hdfs://$hbase_priv_ip:8020
kylin.env.hdfs-working-dir=hdfs://$hbase_priv_ip:8020/kylin

#Spark Configuration
kylin.engine.spark-conf.spark.master=yarn
kylin.engine.spark-conf.spark.submit.deployMode=cluster
kylin.engine.spark-conf.spark.dynamicAllocation.enabled=true
kylin.engine.spark-conf.spark.dynamicAllocation.minExecutors=10
kylin.engine.spark-conf.spark.dynamicAllocation.maxExecutors=1024
kylin.engine.spark-conf.spark.dynamicAllocation.executorIdleTimeout=300
kylin.engine.spark-conf.spark.shuffle.service.enabled=true
kylin.engine.spark-conf.spark.shuffle.service.port=7337
kylin.engine.spark-conf.spark.network.timeout=600
kylin.engine.spark.rdd-partition-cut-mb=100
kylin.engine.spark-conf.spark.yarn.historyServer.allowTracking=true
kylin.dictionary.max.cardinality=10000000
#Spark History Server
kylin.engine.spark-conf.spark.eventLog.dir=hdfs\:///var/log/spark/apps
kylin.engine.spark-conf.spark.history.fs.logDirectory=hdfs\:///var/log/spark/apps

#the memory config
kylin.engine.spark-conf.spark.driver.memory=4G
kylin.engine.spark-conf.spark.executor.memory=8G

#Server mode
kylin.server.mode=all
" >> $KYLIN_HOME/conf/kylin.properties

#specific 2.6.0
cp /usr/lib/hbase/hbase*.jar $KYLIN_HOME/spark/jars/

#Starting Kylin
$KYLIN_HOME/bin/kylin.sh start