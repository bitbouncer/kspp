FROM ubuntu:18.04

WORKDIR /root/

COPY extract/bin  /usr/local/bin
COPY extract/lib  /usr/local/lib
COPY extract/lib64 /usr/local/lib64
COPY extract/runDeps .

RUN runDeps=$(cat runDeps) && \
    echo $runDeps && \
    apt-get update && apt-get install -y $runDeps bash

