#!/bin/bash
pushd ..
docker build -f docker-ubuntu/Dockerfile.build3rdparty --no-cache -tkspp-build3rdparty-ubuntu .
popd


