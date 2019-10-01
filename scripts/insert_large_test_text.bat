call kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 8 --topic kspp_test_text
call kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 8 --topic kspp_test_words

call kafka-console-producer.bat --broker-list localhost:9092 --topic kspp_test_text < pg10.txt


