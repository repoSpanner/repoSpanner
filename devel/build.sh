#! /usr/bin/bash

SRC_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && cd .. && pwd)"

podman build --pull -t repospanner:dev -v $SRC_DIR:/repospanner:z --force-rm=true \
	-f devel/Dockerfile-build
