mkdir tmp && cd tmp

export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib

wget -O protobuf.tar.gz "https://github.com/protocolbuffers/protobuf/releases/download/v3.6.1/protobuf-cpp-3.6.1.tar.gz" && \
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

wget -O grpc.tar.gz "https://github.com/grpc/grpc/archive/v1.20.1.tar.gz" && \
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
cmake -DRAPIDJSON_BUILD_EXAMPLES=OFF -DRAPIDJSON_BUILD_DOC=OFF -DRAPIDJSON_BUILD_TESTS=OFF  -DBUILD_SHARED_LIBS=ON .. && \
sudo make install && \
sudo rm -rf /usr/local/share/doc/RapidJSON && \
cd ../.. && \
rm rapidjson.tar.gz && \
rm -rf rapidjson

wget -O rocksdb.tar.gz "https://github.com/facebook/rocksdb/archive/v5.18.3.tar.gz" && \
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

#wget -O avro.tar.gz "https://github.com/apache/avro/archive/release-1.8.2.tar.gz" && \

wget -O avro.tar.gz "https://github.com/apache/avro/archive/release-1.9.0.tar.gz" && \
mkdir -p avro && \
tar \
  --extract \
  --file avro.tar.gz \
  --directory avro \
  --strip-components 1 && \
cd avro/lang/c++/ && \
mkdir build && \
cd build && \
cmake -DCMAKE_BUILD_TYPE=Release .. -DBUILD_SHARED_LIBS=ON && \
make -j "$(getconf _NPROCESSORS_ONLN)" && \
sudo make install && \
cd ../../../..
rm avro.tar.gz && \
rm -rf arvo

wget -O civetweb.tar.gz "https://github.com/civetweb/civetweb/archive/v1.11.tar.gz" && \
mkdir -p civetweb && \
tar \
  --extract \
  --file civetweb.tar.gz \
  --directory civetweb \
  --strip-components 1 && \
cd civetweb && \
mkdir build_xx && cd build_xx && \
cmake  -DCMAKE_BUILD_TYPE=Release -DCIVETWEB_ENABLE_CXX=ON -DCIVETWEB_ENABLE_SERVER_EXECUTABLE=OFF -DCIVETWEB_BUILD_TESTING=OFF -DBUILD_SHARED_LIBS=ON .. && \
make -j "$(getconf _NPROCESSORS_ONLN)" && \
sudo make install && \
cd ../.. && \
rm civetweb.tar.gz && \
rm -rf civetweb

wget -O cpr.tar.gz "https://github.com/whoshuu/cpr/archive/1.3.0.tar.gz" && \
mkdir -p cpr && \
tar \
  --extract \
  --file cpr.tar.gz \
  --directory cpr \
  --strip-components 1 && \
cd cpr && \
mkdir build && cd build && \
cmake  -DCMAKE_BUILD_TYPE=Release -DUSE_SYSTEM_CURL=ON -DBUILD_CPR_TESTS=OFF -DBUILD_SHARED_LIBS=ON .. && \
make -j "$(getconf _NPROCESSORS_ONLN)" && \
sudo cp lib/libcpr.so /usr/local/lib/libcpr.so && \
sudo mkdir -p /usr/local/include/cpr && \
sudo cp -r ../include/cpr/* /usr/local/include/cpr && \
cd ../.. && \
rm cpr.tar.gz && \
rm -rf cpr

wget -O prometheus-cpp.tar.gz "https://github.com/jupp0r/prometheus-cpp/archive/master.tar.gz" && \
mkdir -p prometheus-cpp && \
tar \
  --extract \
  --file prometheus-cpp.tar.gz \
  --directory prometheus-cpp \
  --strip-components 1 && \
cd prometheus-cpp && \
mkdir build && cd build && \
cmake  -DCMAKE_BUILD_TYPE=Release -DUSE_THIRDPARTY_LIBRARIES=OFF -DENABLE_TESTING=OFF -DBUILD_SHARED_LIBS=ON .. && \
make -j "$(getconf _NPROCESSORS_ONLN)" && \
sudo make install && \
cd ../.. && \
rm prometheus-cpp.tar.gz && \
rm -rf prometheus-cpp


wget -O librdkafka.tar.gz "https://github.com/edenhill/librdkafka/archive/v1.0.0.tar.gz" && \
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



