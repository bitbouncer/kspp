kspp
=========

[![Join the chat at https://gitter.im/kspp/Lobby](https://badges.gitter.im/kspp/Lobby.svg)](https://gitter.im/kspp/Lobby?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

A high performance / realtime C++17 stream-processing framework with avro support. The design is influenced by apache kafka streams library. Change data capture for a postgres and sql server. Export to kafka, postgres, elastic search, influxdb and avro

Sources:
- kafka (uses librdkafka)
- kafka grpc proxy 
- postgres (uses libpq)
- microsoft sqlserver (uses freetds) 
- memory stream

Offset storage:
- kafka
- file
- s3
 
Sinks:
- kafka
- postgres
- influxdb
- elastic search
- avro files
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


## Ubuntu 18.04 x64:

Remove stuff that you should not have...
```
sudo apt-get purge libprotobuf-dev libgrpc++-dev protobuf-compiler
```

Install build tools
```

sudo apt-get install -y software-properties-common
sudo apt-get update
sudo apt-get install -y g++ sudo pax-utils automake autogen shtool libtool git wget cmake unzip build-essential pkg-config sed 
```

Install build deps
```
sudo apt-get install -y python-dev autotools-dev libicu-dev zlib1g-dev openssl libssl-dev libbz2-dev libsnappy-dev \
    libgoogle-glog-dev libgflags-dev libcurl4-openssl-dev libc-ares-dev liblzma-dev libpq-dev freetds-dev libxml2-dev \
    libfmt-dev libpcre2-dev libhttp-parser-dev

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

build docker image / alpine 3:8
```
cd docker-alpine
./build_3rdparty.sh
./build.sh
```

build docker image / ubuntu 18.04
```
cd docker-ubuntu
./build_3rdparty.sh
./build.sh
```


