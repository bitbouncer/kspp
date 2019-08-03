#!/bin/bash

IMAGE_TAG=${1:-latest}

pushd ..
docker build -f docker-alpine/Dockerfile.build3rdparty -tkspp-build3rdparty-alpine:${IMAGE_TAG} .
popd


