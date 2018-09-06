#!/bin/bash
set -ef

cd ../cmake-build-debug/tests/bin

echo "test1"
./test1


echo "test2 mem_counter_store"
./test2_mem_counter_store

echo "test2_mem_store"
./test2_mem_store

echo "test2_mem_windowed_store"
./test2_mem_windowed_store

echo "test2_rocksdb_counter_store"
./test2_rocksdb_counter_store

echo "test2_rocksdb_store"
./test2_rocksdb_store


echo "test2_rocksdb_windowed_store"
./test2_rocksdb_windowed_store

echo "test3_mem_token_bucket"
./test3_mem_token_bucket

echo "test4_kafka_consumer"
./test4_kafka_consumer

echo "test5_kafka_source_sink"
./test5_kafka_source_sink

echo "test6_repartition"
./test6_repartition


echo "test7_cluster_uri"
./test7_cluster_uri

echo "test7_url_vector"
./test7_url_vector

echo "test8_join"
./test8_join

echo "tests OK"






