kspp
=========

[![Join the chat at https://gitter.im/kspp/Lobby](https://badges.gitter.im/kspp/Lobby.svg)](https://gitter.im/kspp/Lobby?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

A high performance / realtime C++ (14) Kafka stream-processing framework based on librdkafka. The design is based on the original Kafka Streams API (java)

Sources:
- kafka
- postgres
- ms sqlserver
 
Sinks:
- kafka
- postgres
- influxdb  

Statestores:
- rocksdb
- memory

It is intended to be run in kubernetes but works equally well standalone

Platforms: Linux


## Ubuntu 18.04 x64:

Install build tools
```
sudo apt-get install -y automake autogen shtool libtool git wget cmake unzip build-essential libboost-all-dev g++ python-dev autotools-dev libicu-dev zlib1g-dev openssl libssl-dev libbz2-dev libsnappy-dev libgoogle-glog-dev libgflags-dev libjansson-dev libcurl4-openssl-dev liblzma-dev libpq-dev pkg-config
```
optional rocksdb 
```
  wget -O rocksdb.tar.gz "https://github.com/facebook/rocksdb/archive/v5.14.3.tar.gz" && \
  mkdir -p rocksdb && \
  tar \
      --extract \
      --file rocksdb.tar.gz \
      --directory rocksdb \
      --strip-components 1 && \
  cd rocksdb && \
  export USE_RTTI=1 && \
  make -j "$(getconf _NPROCESSORS_ONLN)" shared_lib && \
  sudo make install-shared && \
  cd .. && \
  rm rocksdb.tar.gz && \
  rm -rf rocksdb
```

optional freetds 
```
wget -O freetds-patched.tar.gz "ftp://ftp.freetds.org/pub/freetds/stable/freetds-patched.tar.gz" && \
mkdir -p freetds && \
tar \
  --extract \
  --file freetds-patched.tar.gz \
  --directory freetds \
  --strip-components 1 && \
cd freetds && \
./configure --prefix=/usr/local && \
make -j "$(getconf _NPROCESSORS_ONLN)" && \
sudo make install && \
cd .. && \
rm freetds-patched.tar.gz && \
rm -rf freetds
```

Install a late RapidJson, avro and librdkafka
```
wget -O rapidjson.tar.gz "https://github.com/miloyip/rapidjson/archive/v1.1.0.tar.gz" && \
mkdir -p rapidjson && \
tar \
   --extract \
   --file rapidjson.tar.gz \
   --directory rapidjson \
   --strip-components 1 && \
cd rapidjson && \
mkdir build && \
cd build && \
cmake -DRAPIDJSON_BUILD_EXAMPLES=OFF -DRAPIDJSON_BUILD_DOC=OFF -DRAPIDJSON_BUILD_TESTS=OFF .. && \
sudo make install && \
cd ../.. && \
rm rapidjson.tar.gz && \
rm -rf rapidjson

wget -O avro.tar.gz "https://github.com/apache/avro/archive/release-1.8.2.tar.gz" && \
mkdir -p avro && \
tar \
  --extract \
  --file avro.tar.gz \
  --directory avro \
  --strip-components 1 && \
cd avro/lang/c++/ && \
mkdir build && \
cd build && \
cmake -DCMAKE_BUILD_TYPE=Release .. && \
make -j "$(getconf _NPROCESSORS_ONLN)" && \
sudo make install && \
cd ../../../..
rm avro.tar.gz && \
rm -rf arvo

wget -O librdkafka.tar.gz "https://github.com/edenhill/librdkafka/archive/v0.11.5.tar.gz" && \
mkdir -p librdkafka && \
tar \
  --extract \
  --file librdkafka.tar.gz \
  --directory librdkafka \
  --strip-components 1 && \
cd librdkafka && \
./configure --prefix=/usr/local && \
make -j "$(getconf _NPROCESSORS_ONLN)" && \
sudo make install && \
cd .. && \
rm librdkafka.tar.gz && \
rm -rf librdkafka

wget -O google-test.tar.gz "https://github.com/google/googletest/archive/release-1.8.1.tar.gz" && \
mkdir -p google-test && \
tar \
  --extract \
  --file google-test.tar.gz \
  --directory google-test \
  --strip-components 1 && \
cd google-test && \
mkdir build && cd build && \
cmake  -DCMAKE_BUILD_TYPE=Release ..
make -j "$(getconf _NPROCESSORS_ONLN)" && \
sudo make install && \
cd ../.. && \
rm google-test.tar.gz && \
rm -rf google-test

wget -O google-benchmark.tar.gz "https://github.com/google/benchmark/archive/v1.4.1.tar.gz" && \
mkdir -p google-benchmark && \
tar \
  --extract \
  --file google-benchmark.tar.gz \
  --directory google-benchmark \
  --strip-components 1 && \
cd google-benchmark && \
mkdir build && cd build && \
cmake  -DCMAKE_BUILD_TYPE=Release ..
make -j "$(getconf _NPROCESSORS_ONLN)" && \
sudo make install && \
cd ../.. && \
rm google-benchmark.tar.gz && \
rm -rf google-benchmark

https://github.com/civetweb/civetweb/archive/v1.11.tar.gz

wget -O civetweb.tar.gz "https://github.com/civetweb/civetweb/archive/v1.11.tar.gz" && \
mkdir -p civetweb && \
tar \
  --extract \
  --file civetweb.tar.gz \
  --directory civetweb \
  --strip-components 1 && \
mkdir build_xx && cd build_xx && \
cmake  -DCMAKE_BUILD_TYPE=Release -DBUILD_SHARED_LIBS=YES ..
make -j "$(getconf _NPROCESSORS_ONLN)" && \
sudo make install && \
cd ../.. && \
rm civetweb.tar.gz && \
rm -rf civetweb

```

build kspp
```
git clone https://github.com/bitbouncer/kspp.git
cd kspp
mkdir build && cd build
cmake  -DCMAKE_BUILD_TYPE=Release -DLINK_SHARED=ON ..
make -j "$(getconf _NPROCESSORS_ONLN)"
sudo make install
cd ..
```


