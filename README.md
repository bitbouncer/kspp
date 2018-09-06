kspp
=========

[![Join the chat at https://gitter.im/kspp/Lobby](https://badges.gitter.im/kspp/Lobby.svg)](https://gitter.im/kspp/Lobby?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

A high performance / realtime C++ (14) Kafka stream-processing framework based on librdkafka. The design is based on the original Kafka Streams API (java)

It is intended to be run on mesos or kubernetes but works equally well standalone

Platforms: Windows / Linux / Mac


## Ubuntu 18.04 x64:

Install build tools
```
sudo apt-get install -y automake autogen shtool libtool git wget cmake unzip build-essential libboost-all-dev g++ python-dev autotools-dev libicu-dev zlib1g-dev openssl libssl-dev libbz2-dev libsnappy-dev libgoogle-glog-dev libgflags-dev libjansson-dev libcurl4-openssl-dev liblzma-dev libpq-dev pkg-config
```
optional build rocksdb 
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

optional build freetds 
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
```

build kspp
```
git clone https://github.com/bitbouncer/kspp.git
cd kspp
#minimal
./rebuild.sh
#all options
./rebuild-all-options.sh
cd ..
```

## MacOS X

Install build tools (using Homebrew)
```
# Install Xcode
xcode-select --install
brew install cmake
brew install kafka
brew install snappy
brew install rocksdb
brew install boost
brew install boost-python
brew install glog
brew install gflags
brew install rapidjson
brew install avro-cpp
brew install librdkafka
brew install curl --with-openssl # && brew link curl --force
```

Check out source code
```
git clone https://github.com/bitbouncer/kspp.git
cd kspp
```

Run the build
```
#minimal
./rebuild.sh
#all options
./rebuild-all-options.sh
cd ..
```



```

