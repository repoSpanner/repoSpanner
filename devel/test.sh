#! /usr/bin/bash

SRC_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && cd .. && pwd)"

podman run --rm -it -v $SRC_DIR:/repospanner:z repospanner:dev bash -c "cd /repospanner && ./build.sh && go test ./... -failfast -timeout 20m"
