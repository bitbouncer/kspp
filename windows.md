rmdir /S /Q bin\x64 
rmdir /S /Q lib\x64 
rmdir /S /Q include 
rmdir /S /Q build

git config --global core.preloadindex true 
git config --global core.fscache true 
git config --global gc.auto 256 
git config --global core.autocrlf true

mkdir include 
mkdir lib 
mkdir lib\x64 
mkdir lib\x64\Release 
mkdir lib\x64\Debug

set VISUALSTUDIO_VERSION_MAJOR=14
ECHO ===== CMake for 64-bit ====== 
call "C:\Program Files (x86)\Microsoft Visual Studio %VISUALSTUDIO_VERSION_MAJOR%.0\VC\vcvarsall.bat" amd64

cd zlib 
mkdir build & cd build 
cmake -G "Visual Studio 14 Win64" .. 
msbuild zlib.sln 
msbuild zlib.sln /p:Configuration=Release 
cd ../..


cd openssl
git checkout OpenSSL_1_1_0c
git submodule update

perl Configure VC-WIN64A
nmake
#must be run as administrator
nmake install
cd ..

cd rocksdb 
mkdir build & cd build 
cmake -G "Visual Studio 14 Win64" .. 
msbuild rocksdb.sln 
msbuild rocksdb.sln /p:Configuration=Release

cd protobuf 
git clone -b release-1.7.0 https://github.com/google/googlemock.git gmock 
cd gmock git clone -b release-1.7.0 https://github.com/google/googletest.git gtest 
cd ../cmake 
mkdir build & cd build 
mkdir solution & cd solution 
cmake -G "Visual Studio 14 Win64" -DCMAKE_INSTALL_PREFIX=../../../../install ../.. 
msbuild protobuf.sln 
msbuild protobuf.sln /p:Configuration=Release 
cd ../../../..

mkdir include 
mkdir include\librdkafka 
xcopy /e /s librdkafka\src\*.h include\librdkafka 
xcopy /e /s librdkafka\src-cpp\*.h include\librdkafka

