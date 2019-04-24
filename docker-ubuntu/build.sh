#!/bin/bash
set -ef

export BUILD_CONTAINER_NAME=kspp-build-ubuntu
export EXTRACT_CONTAINER=kspp-build-ubuntu-extract
export TAG_NAME=kspp-sample-ubuntu

rm -rf ./extract
mkdir -p ./extract/bin
mkdir -p ./extract/lib
mkdir -p ./extract/lib64
echo "removing old extract container"
docker rm -f $EXTRACT_CONTAINER || true

pushd ..
docker build -f docker-ubuntu/Dockerfile.build  --no-cache -t$BUILD_CONTAINER_NAME .
popd
docker create --name $EXTRACT_CONTAINER $BUILD_CONTAINER_NAME

docker cp $EXTRACT_CONTAINER:/usr/local/lib                                 ./extract
echo $PWD
find ./extract -name "*.a" -exec rm -rf {} \;

docker cp $EXTRACT_CONTAINER:/usr/local/bin/kafka2es                          ./extract/bin
docker cp $EXTRACT_CONTAINER:/usr/local/bin/kafka2influxdb                    ./extract/bin
docker cp $EXTRACT_CONTAINER:/usr/local/bin/kafka2postgres                    ./extract/bin
docker cp $EXTRACT_CONTAINER:/usr/local/bin/postgres2kafka                    ./extract/bin
docker cp $EXTRACT_CONTAINER:/usr/local/bin/tds2kafka                         ./extract/bin
docker cp $EXTRACT_CONTAINER:/usr/local/bin/bb2pg                             ./extract/bin
docker cp $EXTRACT_CONTAINER:/usr/local/bin/bb_generic_avro_console_exporter  ./extract/bin


docker cp $EXTRACT_CONTAINER:/src/runDeps                               ./extract/runDeps

docker rm -f $EXTRACT_CONTAINER

docker build -f Dockerfile --no-cache -t$TAG_NAME .

rm -rf ./extract


