#!/bin/bash

IMAGE_TAG=${1:-latest}

pushd ..
docker build --file docker-ubuntu/Dockerfile.build3rdparty --tag kspp-build3rdparty-ubuntu:${IMAGE_TAG} .
popd




