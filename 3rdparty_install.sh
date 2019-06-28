set -ef 

export AVRO_VER="release-1.9.0"
export AWS_SDK_VER="1.7.128"
export CIVETWEB_VER="v1.11"
export CPR_VER="1.3.0"
export GRPC_VER="v1.21.0"
export LIBRDKAFKA_VER="v1.0.0"
export LIBS3_VER="master"
export PROMETHEUS_CPP_VER="master"
export RAPIDJSON_VER="v1.1.0"
export PROTOBUF_VER="3.7.0"

export ROCKDB_VER="v5.18.3"

export CPP_STANDARD="17"

mkdir tmp && cd tmp

export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib

#wget -O boost.tar.gz "https://dl.bintray.com/boostorg/release/1.70.0/source/boost_1_70_0.tar.gz" && \
#mkdir -p boost && \
#tar \
#  --extract \
#  --file boost.tar.gz \
#  --directory boost \
#  --strip-components 1 && \
#cd boost && \
#./bootstrap.sh  && \
#./b2 cxxflags=-std=c++17 -j "$(getconf _NPROCESSORS_ONLN)" stage && \
#sudo ./b2 cxxflags=-std=c++17 install && \
#cd .. && \
#rm boost.tar.gz && \
#rm -rf boost

wget -O avro.tar.gz "https://github.com/apache/avro/archive/$AVRO_VER.tar.gz" && \
mkdir -p avro && \
tar \
  --extract \
  --file avro.tar.gz \
  --directory avro \
  --strip-components 1 && \
cd avro/lang/c++/ && \
sed -i 's/-std=c++11/-std=c++17/g' CMakeLists.txt && \ 
sed -i '/regex system)/a SET(Boost_LIBRARIES boost_program_options boost_iostreams boost_filesystem boost_regex boost_system z bz2)' CMakeLists.txt && \
mkdir build && cd build && \
cmake -DCMAKE_BUILD_TYPE=Release .. -DBUILD_SHARED_LIBS=ON -DCMAKE_CXX_STANDARD=$CPP_STANDARD && \
make -j "$(getconf _NPROCESSORS_ONLN)" && \
sudo make install && \
cd ../../../.. && \
rm avro.tar.gz && \
rm -rf arvo

wget -O protobuf.tar.gz "https://github.com/protocolbuffers/protobuf/releases/download/v$PROTOBUF_VER/protobuf-cpp-$PROTOBUF_VER.tar.gz" && \
mkdir -p protobuf && \
tar \
  --extract \
  --file protobuf.tar.gz \
  --directory protobuf \
  --strip-components 1 && \
cd protobuf && \
./configure && \
make -j "$(getconf _NPROCESSORS_ONLN)" && \
sudo make install && \
cd .. && \
rm protobuf.tar.gz && \
rm -rf protobuf

wget -O grpc.tar.gz "https://github.com/grpc/grpc/archive/$GRPC_VER.tar.gz" && \
mkdir -p grpc && \
tar \
  --extract \
  --file grpc.tar.gz \
  --directory grpc \
  --strip-components 1 && \
cd grpc && \
make -j "$(getconf _NPROCESSORS_ONLN)" && \
sudo make install && \
cd .. && \
rm grpc.tar.gz && \
rm -rf grpc

wget -O rapidjson.tar.gz "https://github.com/miloyip/rapidjson/archive/$RAPIDJSON_VER.tar.gz" && \
mkdir -p rapidjson && \
tar \
   --extract \
   --file rapidjson.tar.gz \
   --directory rapidjson \
   --strip-components 1 && \
cd rapidjson && \
mkdir build && \
cd build && \
cmake -DRAPIDJSON_BUILD_EXAMPLES=OFF -DRAPIDJSON_BUILD_DOC=OFF -DRAPIDJSON_BUILD_TESTS=OFF -DBUILD_SHARED_LIBS=ON -DCMAKE_CXX_STANDARD=$CPP_STANDARD .. && \
sudo make install && \
sudo rm -rf /usr/local/share/doc/RapidJSON && \
cd ../.. && \
rm rapidjson.tar.gz && \
rm -rf rapidjson

wget -O rocksdb.tar.gz "https://github.com/facebook/rocksdb/archive/$ROCKDB_VER.tar.gz" && \
mkdir -p rocksdb && \
tar \
    --extract \
    --file rocksdb.tar.gz \
    --directory rocksdb \
    --strip-components 1 && \
cd rocksdb && \
export USE_RTTI=1 && \
make -j "$(getconf _NPROCESSORS_ONLN)" shared_lib && \
sudo cp -r include/* /usr/local/include/ && \
sudo cp librocksdb.so /usr/local/lib/ && \
cd .. && \
rm rocksdb.tar.gz && \
rm -rf rocksdb

wget -O civetweb.tar.gz "https://github.com/civetweb/civetweb/archive/$CIVETWEB_VER.tar.gz" && \
mkdir -p civetweb && \
tar \
  --extract \
  --file civetweb.tar.gz \
  --directory civetweb \
  --strip-components 1 && \
cd civetweb && \
mkdir build_xx && cd build_xx && \
cmake  -DCMAKE_BUILD_TYPE=Release -DCIVETWEB_ENABLE_CXX=ON -DCIVETWEB_ENABLE_SERVER_EXECUTABLE=OFF -DCIVETWEB_BUILD_TESTING=OFF -DBUILD_SHARED_LIBS=ON -DCMAKE_CXX_STANDARD=$CPP_STANDARD .. && \
make -j "$(getconf _NPROCESSORS_ONLN)" && \
sudo make install && \
cd ../.. && \
rm civetweb.tar.gz && \
rm -rf civetweb

wget -O cpr.tar.gz "https://github.com/whoshuu/cpr/archive/$CPR_VER.tar.gz" && \
mkdir -p cpr && \
tar \
  --extract \
  --file cpr.tar.gz \
  --directory cpr \
  --strip-components 1 && \
cd cpr && \
mkdir build && cd build && \
cmake  -DCMAKE_BUILD_TYPE=Release -DUSE_SYSTEM_CURL=ON -DBUILD_CPR_TESTS=OFF -DBUILD_SHARED_LIBS=ON -DCMAKE_CXX_STANDARD=$CPP_STANDARD .. && \
make -j "$(getconf _NPROCESSORS_ONLN)" && \
sudo cp lib/libcpr.so /usr/local/lib/libcpr.so && \
sudo mkdir -p /usr/local/include/cpr && \
sudo cp -r ../include/cpr/* /usr/local/include/cpr && \
cd ../.. && \
rm cpr.tar.gz && \
rm -rf cpr

wget -O prometheus-cpp.tar.gz "https://github.com/jupp0r/prometheus-cpp/archive/$PROMETHEUS_CPP_VER.tar.gz" && \
mkdir -p prometheus-cpp && \
tar \
  --extract \
  --file prometheus-cpp.tar.gz \
  --directory prometheus-cpp \
  --strip-components 1 && \
cd prometheus-cpp && \
mkdir build && cd build && \
cmake  -DCMAKE_BUILD_TYPE=Release -DUSE_THIRDPARTY_LIBRARIES=OFF -DENABLE_TESTING=OFF -DBUILD_SHARED_LIBS=ON -DCMAKE_CXX_STANDARD=$CPP_STANDARD .. && \
make -j "$(getconf _NPROCESSORS_ONLN)" && \
sudo make install && \
cd ../.. && \
rm prometheus-cpp.tar.gz && \
rm -rf prometheus-cpp

wget -O aws-sdk.tar.gz "https://github.com/aws/aws-sdk-cpp/archive/$AWS_SDK_VER.tar.gz" && \
mkdir -p aws-sdk && \
tar \
  --extract \
  --file aws-sdk.tar.gz \
  --directory aws-sdk \
  --strip-components 1 && \
cd aws-sdk && \
mkdir build && \
cd build && \
cmake -DCMAKE_BUILD_TYPE=Release -DBUILD_SHARED_LIBS=ON -DBUILD_ONLY="s3" -DCPP_STANDARD=$CPP_STANDARD .. && \
make -j "$(getconf _NPROCESSORS_ONLN)" && \
sudo make install && \
cd ../..

wget -O librdkafka.tar.gz "https://github.com/edenhill/librdkafka/archive/$LIBRDKAFKA_VER.tar.gz" && \
mkdir -p librdkafka && \
tar \
  --extract \
  --file librdkafka.tar.gz \
  --directory librdkafka \
  --strip-components 1 && \
cd librdkafka && \
./configure --prefix=/usr/local && \
make -j "$(getconf _NPROCESSORS_ONLN)" && \
sudo make install && \
cd .. && \
rm librdkafka.tar.gz && \
rm -rf librdkafka

cd ..



