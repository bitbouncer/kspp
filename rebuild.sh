#!/bin/bash
#
rm -rf build bin lib
mkdir build
cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j8
