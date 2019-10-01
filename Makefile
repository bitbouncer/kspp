.PHONY: build
build:
	cd docker-ubuntu && ./build_3rdparty.sh
	cd docker-ubuntu && ./build.sh
