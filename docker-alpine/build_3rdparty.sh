#!/bin/bash
pushd ..
docker build -f docker/Dockerfile.build3rdparty --no-cache -tkspp-build3rdparty-apine .
popd


