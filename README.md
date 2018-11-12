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

Platforms: Linux (Windows and Mac build are outdated)


## Ubuntu 18.04 x64:

Install build tools
```
sudo apt-get install -y automake autogen shtool libtool git wget cmake unzip build-essential pkg-config
```

Install build deps
```
sudo apt-get install -y libboost-all-dev g++ python-dev autotools-dev libicu-dev zlib1g-dev openssl libssl-dev libbz2-dev libsnappy-dev libgoogle-glog-dev libgflags-dev libjansson-dev libcurl4-openssl-dev liblzma-dev libpq-dev freetds-dev libc-ares-dev
```

install 3rd party deps we need to build from source
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
#this takes a long time and is only needed when 3rd party things change
./build_3rdparty.sh

./build.sh
```

build docker image / ubuntu 18.04
```
cd docker-ubuntu
#this takes a long time and is only needed when 3rd party things change
./build_3rdparty.sh

./build.sh
```


