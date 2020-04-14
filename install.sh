#!/bin/bash
rm -rf build bin lib
mkdir build
cd build
sudo rm -rf /usr/local/include/kspp
cmake -DCMAKE_BUILD_TYPE=Release -DBUILD_STATIC_LIBS=OFF -DBUILD_SHARED_LIBS=ON ..
make -j "$(getconf _NPROCESSORS_ONLN)"
sudo make install

