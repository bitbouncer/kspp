kspp
=========

[![Join the chat at https://gitter.im/kspp/Lobby](https://badges.gitter.im/kspp/Lobby.svg)](https://gitter.im/kspp/Lobby?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

A high performance / realtime C++17 stream-processing framework with avro support. The design is influenced by apache kafka streams library. Change data capture for a postgres and sql server. Export to kafka, mqtt, postgres, elastic search, influxdb and avrofiles

Sources:
- kafka (uses librdkafka)
- kafka grpc proxy
- aws kinesis (experimental, using aws sdk) 
- postgres (uses libpq)
- microsoft sqlserver (uses freetds) 
- memory stream

Offset storage:
- kafka
- file
- S3 (uses aws sdk)
 
Sinks:
- kafka (using librdkafka)
- mqtt (using paho libraries)
- postgres (uses libpq)
- influxdb 
- elastic search
- files (avro)
- S3 (avro)
- memory stream

Statestores:
- rocksdb
- memory

Codecs:
- avro (with confluent schema registry or grpc proxy)
- text
- json

Metrics:
- prometheus

It is intended to be run in kubernetes but works equally well standalone

Platforms: Linux (Windows and Mac build are outdated)


## Ubuntu 22.04 x64:

Remove stuff that you should not have...
```
sudo apt-get purge libprotobuf-dev libgrpc++-dev protobuf-compiler
sudo apt-get purge openssl-dev
```

Install build tools
```

sudo apt-get install -y software-properties-common
sudo apt-get update
sudo apt-get install -y g++ sudo pax-utils automake autogen shtool libtool git wget cmake unzip build-essential pkg-config sed bison flex
```

Install build deps
```
sudo apt-get install -y python-dev autotools-dev libicu-dev zlib1g-dev openssl libssl-dev libbz2-dev libsnappy-dev liblz4-dev libzstd-dev \
    libgflags-dev libcurl4-openssl-dev libc-ares-dev liblzma-dev libpq-dev freetds-dev libxml2-dev \
    libfmt-dev libpcre2-dev libhttp-parser-dev 
    
#    libgoogle-glog-dev missing packet for u22.04    

```

install 3rd party deps we need to build from source  (you can hack versions - see the file)
```
./3rdparty_install.sh 

```

build kspp
```
mkdir build && cd build
cmake  -DCMAKE_BUILD_TYPE=Release -DBUILD_SHARED_LIBS=ON -DLINK_SHARED=ON ..
make -j "$(getconf _NPROCESSORS_ONLN)"
sudo make install
cd ..
```

build docker image / ubuntu 22.04
```
cd docker-ubuntu
./build_3rdparty.sh
./build.sh
```

