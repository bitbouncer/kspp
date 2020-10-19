#!/bin/bash

IMAGE_TAG=${1:-latest}

pushd ..
docker build --file docker/Dockerfile.build3rdparty --tag kspp-build3rdparty:${IMAGE_TAG} .
popd




