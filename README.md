# Omega

# How to Run

1. Download Kafka 0.10.1 and run 2 nodes and ZK. Change the data dirs for ZK & Kafka.

		bin/zookeeper-server-start.sh src/main/resources/zookeeper.properties
		bin/kafka-server-start.sh src/main/resources/kafka-foo.properties
		bin/zookeeper-server-start.sh src/main/resources/kafka-bar.properties
		
2. Run the `io.omega.KafkaProxyServer` in the **IntelliJ Idea**. Run the class `Run > Run 'KafkaProxyServer'`and add `<path to workspace>/omega/src/main/resources/proxy.properties` as arguments in the `Run > Edit configuration`.

3. Run the following to test

		 kafkacat -L -b localhost:9088
		 seq 10000 | kafkacat -P -b localhost:9088 -p -1 -t foo
		 kafkacat -C -b localhost:9088 -t foo
		 
		 bin/kafka-topics.sh --zookeeper localhost:2181/boof --create --topic bar --replication-factor 1 --partitions 10
		 seq 10000 | kafkacat -P -b localhost:9088 -p -1 -t bar
		 bin/kafka-console-consumer.sh --topic bar --bootstrap-server localhost:9088 --new-consumer --from-beginning --max-messages 20

