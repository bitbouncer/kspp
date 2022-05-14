## Windows x64:

Note: this is unmaintained... pull requests welcome

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
git checkout v5.7.5
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
git checkout v0.11.3
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
