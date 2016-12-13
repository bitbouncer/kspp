call C:\[KAFKA_DEV]\kafka_2.11-0.10.0.1\bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 8 --topic kspp_test0_table --config cleanup.policy=compact
call C:\[KAFKA_DEV]\kafka_2.11-0.10.0.1\bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 8 --topic kspp_test0_eventstream --config retention.ms=86400000
call C:\[KAFKA_DEV]\kafka_2.11-0.10.0.1\bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 8 --topic kspp_test0_eventstream_out --config retention.ms=86400000

call C:\[KAFKA_DEV]\kafka_2.11-0.10.0.1\bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 8 --topic kspp_PageViews
call C:\[KAFKA_DEV]\kafka_2.11-0.10.0.1\bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 8 --topic kspp_UserProfile
call C:\[KAFKA_DEV]\kafka_2.11-0.10.0.1\bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 8 --topic kspp_ViewCountsByUser
call C:\[KAFKA_DEV]\kafka_2.11-0.10.0.1\bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 8 --topic kspp_PageViewByRegion
call C:\[KAFKA_DEV]\kafka_2.11-0.10.0.1\bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 8 --topic kspp_UserCountByRegion


call C:\[KAFKA_DEV]\kafka_2.11-0.10.0.1\bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 8 --topic kspp_PageCounts
call C:\[KAFKA_DEV]\kafka_2.11-0.10.0.1\bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 8 --topic kspp_PageViewsDecorated

call C:\[KAFKA_DEV]\kafka_2.11-0.10.0.1\bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 8 --topic kspp_TextInput
call C:\[KAFKA_DEV]\kafka_2.11-0.10.0.1\bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 8 --topic kspp_WordCount

