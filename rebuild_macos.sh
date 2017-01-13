#!/bin/bash
#
# Builds the project on MacOS
# (tested on Sierra 10.12.2)
#

rm -rf build bin lib
mkdir build
cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
make
