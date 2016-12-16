rm -rf build bin lib
mkdir build
cd build
cmake -D__LINUX__=1 -DCMAKE_BUILD_TYPE=Release ..
make 
cd ..

