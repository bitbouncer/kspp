FROM alpine:3.8

WORKDIR /root/

RUN echo "http://nl.alpinelinux.org/alpine/edge/testing" >>/etc/apk/repositories

COPY extract/bin  /usr/local/bin
COPY extract/lib  /usr/local/lib
COPY extract/lib64 /usr/local/lib64
COPY extract/runDeps .

RUN runDeps=$(cat runDeps) && \
    echo $runDeps && \
    apk add --no-cache $runDeps bash

