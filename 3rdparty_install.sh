set -ef 

export CPP_STANDARD="17"

export AVRO_VER="release-1.9.0"
export AWS_SDK_VER="1.7.220"
export GRPC_VER="v1.22.1"
export LIBRDKAFKA_VER="v1.1.0"
export PROMETHEUS_CPP_VER="v0.7.0"
export RAPIDJSON_VER="v1.1.0"
export NLOHMANN_JSON_VER="3.7.1"
export PROTOBUF_VER="3.7.0"
export ROCKDB_VER="v5.18.3"

#deps for arrow
export DOUBLE_CONVERSION_VER="v3.1.5"
export BROTLI_VER="v1.0.7"
export FLATBUFFERS_VER="v1.11.0"
export THRIFT_VER="0.12.0"
export ARROW_VER="apache-arrow-0.14.1"

export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib

rm -rf tmp
mkdir tmp
cd tmp


wget -O boost.tar.gz "https://dl.bintray.com/boostorg/release/1.70.0/source/boost_1_70_0.tar.gz" && \
mkdir -p boost && \
tar \
  --extract \
  --file boost.tar.gz \
  --directory boost \
  --strip-components 1
cd boost
./bootstrap.sh
./b2 cxxstd=17 --with-program_options --with-iostreams --with-filesystem --with-regex --with-system --with-date_time  -j "$(getconf _NPROCESSORS_ONLN)" stage
sudo ./b2 cxxstd=17 --with-program_options --with-iostreams --with-filesystem --with-regex --with-system --with-date_time install
cd ..
rm boost.tar.gz
rm -rf boost

wget -O avro.tar.gz "https://github.com/apache/avro/archive/$AVRO_VER.tar.gz"
mkdir -p avro
tar \
  --extract \
  --file avro.tar.gz \
  --directory avro \
  --strip-components 1
sed -i.bak1 's/-std=c++11/-std=c++17/g' avro/lang/c++/CMakeLists.txt
sed -i.bak2 '/regex system)/a SET(Boost_LIBRARIES boost_program_options boost_iostreams boost_filesystem boost_regex boost_system z bz2)' avro/lang/c++/CMakeLists.txt
#sed -i.bak3 '/find_package (Boost/d' avro/lang/c++/CMakeLists.txt
#sed -i.bak4 '/regex system)/d' avro/lang/c++/CMakeLists.txt
cat avro/lang/c++/CMakeLists.txt
cd avro/lang/c++/ 
mkdir build 
cd build
cmake -DCMAKE_BUILD_TYPE=Release .. -DBUILD_SHARED_LIBS=ON -DCMAKE_CXX_STANDARD=$CPP_STANDARD
make -j "$(getconf _NPROCESSORS_ONLN)"
sudo make install
cd ../../../..
rm avro.tar.gz
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
    --strip-components 1
cd rocksdb
export USE_RTTI=1
make -j "$(getconf _NPROCESSORS_ONLN)" shared_lib
sudo make install-shared
cd ..
rm rocksdb.tar.gz
rm -rf rocksdb

wget -O prometheus-cpp.tar.gz "https://github.com/jupp0r/prometheus-cpp/archive/$PROMETHEUS_CPP_VER.tar.gz" && \
mkdir -p prometheus-cpp && \
tar \
  --extract \
  --file prometheus-cpp.tar.gz \
  --directory prometheus-cpp \
  --strip-components 1 && \
cd prometheus-cpp
mkdir build && cd build
cmake  -DCMAKE_BUILD_TYPE=Release -DENABLE_PULL=OFF -DUSE_THIRDPARTY_LIBRARIES=OFF -DENABLE_TESTING=OFF -DBUILD_SHARED_LIBS=ON -DOVERRIDE_CXX_STANDARD_FLAGS=OFF -DCMAKE_CXX_STANDARD=$CPP_STANDARD ..
make -j "$(getconf _NPROCESSORS_ONLN)"
sudo make install
cd ../..
rm prometheus-cpp.tar.gz
rm -rf prometheus-cpp

wget -O aws-sdk.tar.gz "https://github.com/aws/aws-sdk-cpp/archive/$AWS_SDK_VER.tar.gz" && \
mkdir -p aws-sdk && \
tar \
  --extract \
  --file aws-sdk.tar.gz \
  --directory aws-sdk \
  --strip-components 1
cd aws-sdk
mkdir build
cd build
cmake -DCMAKE_BUILD_TYPE=Release -DBUILD_SHARED_LIBS=ON -DBUILD_ONLY="s3;kinesis;iot" -DCPP_STANDARD=$CPP_STANDARD ..
make -j "$(getconf _NPROCESSORS_ONLN)"
sudo make install
cd ../..

wget -O librdkafka.tar.gz "https://github.com/edenhill/librdkafka/archive/$LIBRDKAFKA_VER.tar.gz" && \
mkdir -p librdkafka && \
tar \
  --extract \
  --file librdkafka.tar.gz \
  --directory librdkafka \
  --strip-components 1
cd librdkafka
./configure --prefix=/usr/local
make -j "$(getconf _NPROCESSORS_ONLN)"
sudo make install
cd ..
rm librdkafka.tar.gz
rm -rf librdkafka

wget -O double-conversion.tar.gz "https://github.com/google/double-conversion/archive/$DOUBLE_CONVERSION_VER.tar.gz" && \
mkdir -p double-conversion && \
tar \
  --extract \
  --file double-conversion.tar.gz \
  --directory double-conversion \
  --strip-components 1
cd double-conversion
mkdir build
cd build
cmake -DCMAKE_BUILD_TYPE=Release -DBUILD_SHARED_LIBS=ON -DCMAKE_CXX_STANDARD=$CPP_STANDARD ..
make -j "$(getconf _NPROCESSORS_ONLN)"
sudo make install
cd ../..
rm double-conversion.tar.gz

wget -O brotli.tar.gz "https://github.com/google/brotli/archive/$BROTLI_VER.tar.gz" && \
mkdir -p brotli && \
tar \
  --extract \
  --file brotli.tar.gz \
  --directory brotli \
  --strip-components 1
cd brotli
mkdir build
cd build
cmake -DCMAKE_BUILD_TYPE=Release -DBUILD_SHARED_LIBS=ON -DCMAKE_CXX_STANDARD=$CPP_STANDARD ..
make -j "$(getconf _NPROCESSORS_ONLN)"
sudo make install
cd ../..
rm brotli.tar.gz


wget -O flatbuffers.tar.gz "https://github.com/google/flatbuffers/archive/$FLATBUFFERS_VER.tar.gz" && \
mkdir -p flatbuffers && \
tar \
  --extract \
  --file flatbuffers.tar.gz \
  --directory flatbuffers \
  --strip-components 1
cd flatbuffers
mkdir build
cd build
cmake -DCMAKE_BUILD_TYPE=Release -DBUILD_SHARED_LIBS=ON -DFLATBUFFERS_BUILD_TESTS=OFF -DCMAKE_CXX_STANDARD=$CPP_STANDARD ..
make -j "$(getconf _NPROCESSORS_ONLN)"
sudo make install
cd ../..
rm flatbuffers.tar.gz

wget -O thrift.tar.gz "https://github.com/apache/thrift/archive/$THRIFT_VER.tar.gz" && \
mkdir -p thrift && \
tar \
  --extract \
  --file thrift.tar.gz \
  --directory thrift \
  --strip-components 1
cd thrift
./bootstrap.sh
./configure CXXFLAGS='-g -O2' --with-boost=/usr/local --without-nodejs --without-python --without-lua --without-go --without-java --enable-tests=no --enable-static=no
#thrift seems to have probles when doing parallell compliation
make -j 1
sudo make install
cd ..
rm thrift.tar.gz


wget -O arrow.tar.gz "https://github.com/apache/arrow/archive/$ARROW_VER.tar.gz" && \
mkdir -p arrow && \
tar \
  --extract \
  --file arrow.tar.gz \
  --directory arrow \
  --strip-components 1
cd arrow/cpp
mkdir build
cd build
cmake -DCMAKE_BUILD_TYPE=Release -DBUILD_SHARED_LIBS=ON -DARROW_PARQUET=ON -DARROW_DEPENDENCY_SOURCE=SYSTEM -DCMAKE_CXX_STANDARD=$CPP_STANDARD ..
make -j "$(getconf _NPROCESSORS_ONLN)"
sudo make install
cd ../../..
rm arrow.tar.gz


wget -O nlomann.tar.gz "https://github.com/nlohmann/json/archive/v$NLOHMANN_JSON_VER.tar.gz" && \
mkdir -p nlomann && \
tar \
  --extract \
  --file nlomann.tar.gz \
  --directory nlomann \
  --strip-components 1 && \
cd nlomann && \
mkdir build && cd build
cmake ..
make -j "$(getconf _NPROCESSORS_ONLN)" && \
sudo make install && \
cd ../.. && \
rm nlomann.tar.gz && \
rm -rf nlomann

#out of tmp
cd ..
rm -rf tmp



