# kafka_storm_hbase_kafka
kafka-storm-hbase-kafka-integration : Ingest event data and generate alert based on a particular event...

Hadoop : HDP2.5
Sevices Running : Zookeeper, HDFS, YARN, Kafka, Storm, Hbase

Create Topic below:-

cd /usr/hdp/2.5.6.0-40/kafka/bin

./kafka-topics.sh --create --zookeeper victoria.com:2181 --partitions 2 --replication-factor 1 --topic cartEvnt

./kafka-topics.sh --create --zookeeper victoria.com:2181 --partitions 2 --replication-factor 1 --topic shopAlert

Sample Data for cartEvnt:

101,1000,item1,10,add

102,1000,item2,10,add

105,1000,item5,10,del

106,1000,item6,10,add

Hbase Table:
create 'ecomapp', 'cartinfo'

Publish message to Kafka:-
./kafka-console-producer.sh --broker-list victoria.com:6667 --topic cartEvnt

101,1000,item1,10,add

Run Storm Jar:
In Local Mode:
 storm jar target/kafka-0.0.1-SNAPSHOT-jar-with-dependencies.jar apacheStorm.builder.LogAnalyserStorm
In Cluster Mode:
 storm jar target/kafka-0.0.1-SNAPSHOT-jar-with-dependencies.jar apacheStorm.builder.LogAnalyserStorm victoria.com
 
