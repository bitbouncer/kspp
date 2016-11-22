kspp
=========

A C++11 kafka streams library 

This is work-in-progress


Platforms: Windows / Linux

## Ubuntu 16 x64:

Install build tools
```
sudo apt-get install -y automake autogen shtool libtool git wget cmake unzip build-essential libboost-all-dev g++ python-dev autotools-dev libicu-dev zlib1g-dev openssl libssl-dev libbz2-dev

```
Build
```

git clone https://github.com/facebook/rocksdb.git
git clone https://github.com/edenhill/librdkafka.git
git clone https://github.com/bitbouncer/kspp.git

cd kspp
bash -e build_linux.sh
cd ..
```


## Ubuntu 14 x64:

Install build tools
```
sudo apt-get install -y automake autogen shtool libtool git wget cmake unzip build-essential g++ python-dev autotools-dev libicu-dev zlib1g-dev openssl libssl-dev libbz2-dev

```
Build
```
mkdir source && cd source
wget http://sourceforge.net/projects/boost/files/boost/1.62.0/boost_1_62_0.tar.gz/download -Oboost_1_62_0.tar.gz
tar xf boost_1_62_0.tar.gz

cd boost_1_62_0
./bootstrap.sh
./b2 -j 8
cd ..

git clone https://github.com/facebook/rocksdb.git
git clone https://github.com/edenhill/librdkafka.git
git clone https://github.com/bitbouncer/kspp.git

cd kspp
bash -e build_linux.sh
cd ..
```

 
