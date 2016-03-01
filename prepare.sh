bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test
bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test <<EOF
message 1
message 2
EOF