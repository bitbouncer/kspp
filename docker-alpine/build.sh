#!/bin/bash
set -ef

rm -rf ./extract
mkdir -p ./extract/bin
mkdir -p ./extract/lib
mkdir -p ./extract/lib64

pushd ..
docker build -f docker-alpine/Dockerfile.build  --no-cache -tkspp-build-alpine .

popd
docker create --name extract-alpine kspp-build-alpine
docker cp extract-alpine:/usr/local/lib/libavrocpp.so.1.8.2.0       ./extract/lib
docker cp extract-alpine:/usr/local/lib/libbenchmark.so.0.0.0       ./extract/lib
docker cp extract-alpine:/usr/local/lib/libbenchmark_main.so.0.0.0  ./extract/lib
docker cp extract-alpine:/usr/local/lib/libcivetweb-cpp.so.1.11.0   ./extract/lib
docker cp extract-alpine:/usr/local/lib/libcivetweb.so.1.11.0       ./extract/lib
docker cp extract-alpine:/usr/local/lib/libcpr.so                   ./extract/lib
docker cp extract-alpine:/usr/local/lib/libkspp.so                  ./extract/lib
docker cp extract-alpine:/usr/local/lib/libkspp_es.so               ./extract/lib
docker cp extract-alpine:/usr/local/lib/libkspp_influxdb.so         ./extract/lib
docker cp extract-alpine:/usr/local/lib/libkspp_pg.so               ./extract/lib
docker cp extract-alpine:/usr/local/lib/libkspp_rocksdb.so          ./extract/lib
docker cp extract-alpine:/usr/local/lib/libkspp_tds.so              ./extract/lib
docker cp extract-alpine:/usr/local/lib/librdkafka++.so.1           ./extract/lib
docker cp extract-alpine:/usr/local/lib/librdkafka.so.1             ./extract/lib
docker cp extract-alpine:/usr/local/lib/librocksdb.so               ./extract/lib

docker cp extract-alpine:/usr/local/lib64/libgmock.so               ./extract/lib64
docker cp extract-alpine:/usr/local/lib64/libgtest.so               ./extract/lib64
docker cp extract-alpine:/usr/local/lib64/libgmock_main.so          ./extract/lib64
docker cp extract-alpine:/usr/local/lib64/libgtest_main.so          ./extract/lib64
docker cp extract-alpine:/usr/local/lib64/libprometheus-cpp-core.so ./extract/lib64
docker cp extract-alpine:/usr/local/lib64/libprometheus-cpp-pull.so ./extract/lib64
docker cp extract-alpine:/usr/local/lib64/libprometheus-cpp-push.so ./extract/lib64

docker cp extract-alpine:/usr/local/bin/kafka2es                    ./extract/bin
docker cp extract-alpine:/usr/local/bin/kafka2influxdb              ./extract/bin
docker cp extract-alpine:/usr/local/bin/kafka2postgres              ./extract/bin
docker cp extract-alpine:/usr/local/bin/postgres2kafka              ./extract/bin
docker cp extract-alpine:/usr/local/bin/tds2kafka                   ./extract/bin

docker cp extract-alpine:/src/runDeps                               ./extract/runDeps

docker rm -f extract-alpine

docker build -f Dockerfile --no-cache -tkspp-sample-alpine .

rm -rf ./extract


