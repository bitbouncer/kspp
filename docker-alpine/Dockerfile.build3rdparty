FROM alpine:3.8

WORKDIR /src

MAINTAINER sk svante.karlsson@csi.se

#RUN echo "@testing http://nl.alpinelinux.org/alpine/edge/testing" >>/etc/apk/repositories
RUN echo "http://nl.alpinelinux.org/alpine/edge/testing" >>/etc/apk/repositories

RUN apk add \
      sudo ca-certificates git wget tar bash g++ make cmake python perl build-base linux-headers jemalloc \
      libressl-dev glog-dev boost-dev musl-dev zlib-dev \
      curl-dev zlib-dev bzip2-dev snappy-dev lz4-dev zstd-dev c-ares-dev \
      postgresql-dev freetds-dev glog-dev gflags-dev

COPY 3rdparty_install.sh 	.
RUN ./3rdparty_install.sh && \
   strip --strip-all /usr/local/lib64/*.so* && \
   strip --strip-all /usr/local/lib/*.so* && \
   strip --strip-unneeded /usr/local/bin/*


