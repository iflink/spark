1.tool
  eclipse-java-oxygen-3a
  scala-eclipse-plugin -> file:/*/scala-update-site
  
  mvn clean scala:compile compile package

2.kafka command
  kafka-topics.sh --create --zookeeper master:2181,slave1:2181,slave2:2181 --replication-factor 1 --partitions 1 --topic stream_kafka
  kafka-topics.sh --describe --zookeeper master:2181,slave1:2181,slave2:2181 --topic stream_kafka
  kafka-console-producer.sh --broker-list master:9092 --topic stream_kafka
  kafka-console-consumer.sh --zookeeper master:2181,slave1:2181,slave2:2181 --from-beginning --topic stream_kafka
