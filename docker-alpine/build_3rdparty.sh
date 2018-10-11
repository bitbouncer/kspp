#!/bin/bash
pushd ..
docker build -f docker-alpine/Dockerfile.build3rdparty --no-cache -tkspp-build3rdparty-alpine .
popd


