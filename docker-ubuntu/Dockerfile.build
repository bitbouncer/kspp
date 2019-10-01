FROM kspp-build3rdparty-ubuntu:latest
WORKDIR /src

MAINTAINER sk svante.karlsson@csi.se

COPY cmake 	 cmake
COPY examples    examples
COPY include     include
COPY src         src
COPY tests       tests
COPY tools       tools
COPY proto       proto
COPY CMakeLists.txt    .
COPY kspp_config.h.in  .

RUN mkdir build && \
    cd build && \
    cmake  -DCMAKE_BUILD_TYPE=Release -DENABLE_ROCKSDB=ON -DENABLE_POSTGRES=ON -DENABLE_TDS=ON -DENABLE_ELASTICSEARCH=ON -DBUILD_TOOLS=ON -DBUILD_SAMPLES=OFF -DBUILD_TESTS=OFF -DBUILD_STATIC_LIBS=OFF -DBUILD_SHARED_LIBS=ON -DLINK_SHARED=ON .. && \
    make -j "$(getconf _NPROCESSORS_ONLN)" && \
    make install && \
    strip --strip-all /usr/local/lib/*.so* && \
    strip --strip-unneeded /usr/local/bin/*

RUN runDeps="$( \
      scanelf --needed --nobanner --recursive /usr/local \
        | awk '{ gsub(/,/, "\n", $2); print $2 }' \
        | sort -u \
        | xargs -r dpkg -S | cut -d : -f 1  \
        | sort -u \
      )" && \
     echo "$runDeps" > runDeps


