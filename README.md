kspp
=========

A high performance / realtime C++ (14) Kafka stream-processing framework based on librdkafka. The design is based on the original Kafka Streams API (java)

It is intended to be run on mesos or kubernetes but works equally well standalone

Platforms: Windows / Linux / Mac


## Ubuntu 16 x64:

Install build tools
```
sudo apt-get install -y automake autogen shtool libtool git wget cmake unzip build-essential libboost-all-dev g++ python-dev autotools-dev libicu-dev zlib1g-dev openssl libssl-dev libbz2-dev libsnappy-dev libgoogle-glog-dev libgflags-dev libjansson-dev libcurl4-openssl-dev liblzma-dev pkg-config
```
optional build rocksdb 
```
git clone https://github.com/facebook/rocksdb.git
cd rocksdb
git checkout v5.6.1
make -j8 shared_lib
sudo make install-shared
cd ..
```

Install a late RapidJson, avro and librdkafka
```
wget -O rapidjson.tar.gz "https://github.com/miloyip/rapidjson/archive/v1.1.0.tar.gz" && \
   mkdir -p rapidjson && \
   tar \
      --extract \
      --file rapidjson.tar.gz \
      --directory rapidjson \
      --strip-components 1 && \
 cd rapidjson && \
   mkdir build && \
   cd build && \
   cmake -DRAPIDJSON_BUILD_EXAMPLES=OFF -DRAPIDJSON_BUILD_DOC=OFF -DRAPIDJSON_BUILD_TESTS=OFF .. && \
   sudo make install && \
 cd ../.. && \
 rm rapidjson.tar.gz && \
 rm -rf rapidjson

git clone https://github.com/apache/avro.git
cd avro
git checkout release-1.8.2
cd lang/c++/
mkdir build
cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j8
sudo make install
cd ../../../..

git clone https://github.com/edenhill/librdkafka.git
cd librdkafka
git checkout v0.11.0
./configure
make -j8
sudo make install
cd ..
```

build kspp
```
git clone https://github.com/bitbouncer/kspp.git
cd kspp
./rebuild.sh
cd ..
```

## MacOS X

Install build tools (using Homebrew)
```
# Install Xcode
xcode-select --install
brew install cmake
brew install kafka
brew install snappy
brew install rocksdb
brew install boost
brew install boost-python
```

Check out source code
```
git clone https://github.com/bitbouncer/kspp.git
```

Run the build
```
./rebuild.sh
```

## Windows x64:

Install build tools
```
- CMake (https://cmake.org/)
- Visual Studio 14 (https://www.visualstudio.com/downloads/)
- nasm (https://sourceforge.net/projects/nasm/)
- perl (http://www.activestate.com/activeperl)
```
Build
```
wget --no-check-certificate http://downloads.sourceforge.net/project/boost/boost/1.62.0/boost_1_62_0.zip
unzip boost_1_62_0.zip
rename boost_1_62_0 boost
rm -f boost_1_62_0.zip

git clone https://github.com/facebook/rocksdb.git
git clone https://github.com/madler/zlib.git
git clone https://github.com/lz4/lz4.git
git clone https://github.com/openssl/openssl.git
git clone https://github.com/edenhill/librdkafka.git
git clone https://github.com/curl/curl.git
git clone https://github.com/apache/avro.git
git clone https://github.com/miloyip/rapidjson.git
git clone https://github.com/google/glog.git
git clone https://github.com/bitbouncer/kspp.git

set VISUALSTUDIO_VERSION_MAJOR=14
"C:\Program Files (x86)\Microsoft Visual Studio %VISUALSTUDIO_VERSION_MAJOR%.0\VC\vcvarsall.bat" amd64

cd openssl
git checkout OpenSSL_1_1_0f
perl Configure VC-WIN64A
nmake
#you need to be Administrator for the next step)
nmake install 
cd ..

cd rocksdb
git checkout v5.6.1
mkdir build & cd build
cmake -G "Visual Studio 14 Win64" ..
msbuild /maxcpucount:8 rocksdb.sln
msbuild /maxcpucount:8 rocksdb.sln /p:Configuration=Release
cd ../..

cd zlib
rm -rf build
mkdir build & cd build
cmake -G "Visual Studio 14 Win64" ..
msbuild zlib.sln
msbuild zlib.sln /p:Configuration=Release
copy /y zconf.h ..
cd ../..

cd boost
call bootstrap.bat
.\b2.exe -j8 -toolset=msvc-%VisualStudioVersion% variant=release,debug link=static threading=multi runtime-link=shared address-model=64 architecture=x86 --stagedir=stage\lib\x64 stage -s ZLIB_SOURCE=%CD%\..\zlib headers log_setup log date_time timer thread system program_options filesystem regex chrono iostreams
cd ..

cd librdkafka
git checkout v0.11.0
cd ..

mkdir include
mkdir include\librdkafka
xcopy /e /s librdkafka\src\*.h include\librdkafka
xcopy /e /s librdkafka\src-cpp\*.h include\librdkafka

cd curl
git checkout curl-7_55_1
rmdir /s /q builds
rm  libs\x64\Debug\libcurl.lib
rm  libs\x64\Release\libcurl.lib
cd winbuild
nmake /f makefile.vc mode=static VC=%VISUALSTUDIO_VERSION_MAJOR% ENABLE_SSPI=yes ENABLE_WINSSL=yes ENABLE_IDN=no DEBUG=yes MACHINE=x64
nmake /f makefile.vc mode=static VC=%VISUALSTUDIO_VERSION_MAJOR% ENABLE_SSPI=yes ENABLE_WINSSL=yes ENABLE_IDN=no DEBUG=no MACHINE=x64
cd ..
echo CURL COPYING LIBS
mkdir libs
mkdir libs\x64
mkdir libs\x64\Debug
mkdir libs\x64\Release
copy builds\libcurl-vc%VISUALSTUDIO_VERSION_MAJOR%-x64-debug-static-ipv6-sspi-winssl\lib\libcurl_a_debug.lib  libs\x64\Debug\libcurl.lib
copy builds\libcurl-vc%VISUALSTUDIO_VERSION_MAJOR%-x64-release-static-ipv6-sspi-winssl\lib\libcurl_a.lib libs\x64\Release\libcurl.lib
cd ..

cd avro
git checkout release-1.8.2
cd lang/c++/
rm -rf build
mkdir build & cd build
cmake -G "Visual Studio 14 Win64" -DBOOST_ROOT=../../../boost -DBOOST_LIBRARYDIR=..\..\..\boost\stage\lib\x64\lib -DBoost_USE_STATIC_LIBS=TRUE ..
#-DBoost_DEBUG=ON
msbuild /maxcpucount:8 avrocpp_s.vcxproj
msbuild /maxcpucount:8 avrocpp_s.vcxproj /p:Configuration=Release
cd ..
rm -rf include
mkdir include
mkdir include\avro
mkdir include\avro\buffer
copy /y api\*.hh include\avro
copy /y api\buffer\*.hh include\avro\buffer
cd ../../..

cd glog
mkdir build
cd build
cmake -G "Visual Studio %VISUALSTUDIO_VERSION_MAJOR% Win64" -DBUILD_SHARED_LIBS=1 ..
msbuild ALL_BUILD.vcxproj /p:Configuration=Debug /p:Platform=x64 /maxcpucount:12
msbuild ALL_BUILD.vcxproj /p:Configuration=Release /p:Platform=x64 /maxcpucount:12
cd ..
cd ..

cd kspp
call rebuild_windows_vs14-all-options.bat
cd ..

```

