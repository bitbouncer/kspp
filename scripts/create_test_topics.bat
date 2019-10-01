REM kafka-topics.bat must be in you path.. it's somewhere like C:\???\kafka_2.11-0.10.0.1\bin\windows\kafka-topics.bat
call kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 8 --topic kspp_test0_table --config cleanup.policy=compact
call kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 8 --topic kspp_test0_eventstream --config retention.ms=86400000
call kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 8 --topic kspp_test0_eventstream_out --config retention.ms=86400000
call kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 8 --topic kspp_PageViews
call kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 8 --topic kspp_UserProfile
call kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 8 --topic kspp_ViewCountsByUser
call kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 8 --topic kspp_PageViewByRegion
call kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 8 --topic kspp_UserCountByRegion
call kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 8 --topic kspp_PageCounts
call kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 8 --topic kspp_PageViewsDecorated
call kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 8 --topic kspp_TextInput
call kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 8 --topic kspp_WordCount
call kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 8 --topic kspp_example5_usernames
call kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 8 --topic kspp_example5_user_channel
call kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 8 --topic kspp_example5_channel_names
call kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 8 --topic kspp_example5_usernames.per-channel
