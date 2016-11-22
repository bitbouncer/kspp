kspp
=========

A C++11 kafka streams library 

This is work-in-progress


Platforms: Windows / Linux

## Ubuntu 16 x64:

Install build tools
```
sudo apt-get install -y automake autogen shtool libtool git wget cmake unzip build-essential libboost-all-dev g++ python-dev autotools-dev libicu-dev zlib1g-dev openssl libssl-dev libbz2-dev libsnappy-dev

```
Build
```

git clone https://github.com/facebook/rocksdb.git
cd rocksdb
make static_lib

git clone https://github.com/edenhill/librdkafka.git
cd librdkafka
./configure
make
sudo make install
cd ..

git clone https://github.com/bitbouncer/kspp.git
cd kspp
mkdir build && cd build
cmake -D__LINUX__=1 ..
make
cd ..
```
