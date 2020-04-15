#!/bin/bash
set -e 

rm -rf build bin lib
mkdir build
cd build
sudo rm -rf /usr/local/include/kspp
sudo rm -rf /usr/local/lib/libkspp*.so
cmake -DCMAKE_BUILD_TYPE=Release -DBUILD_STATIC_LIBS=OFF -DBUILD_SHARED_LIBS=ON -DLINK_SHARED=ON -DBUILD_TESTS=OFF -DBUILD_SAMPLES=OFF ..
make -j "$(getconf _NPROCESSORS_ONLN)"
sudo make install

