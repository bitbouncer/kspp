rm -rf build bin lib
mkdir build
cd build
cmake -DALPINE_LINUX=1 -DCMAKE_BUILD_TYPE=Release ..
make 
cd ..

