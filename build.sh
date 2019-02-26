#!/bin/sh -xe
export VERSION="$(cat VERSION)"
export GITDESCRIP="$(git describe --long --tags --dirty --always)"

(
    cd cmd/repospanner/
    go build -ldflags \
        "-X repospanner.org/repospanner/server/constants.version=$VERSION
        -X repospanner.org/repospanner/server/constants.gitdescrip=$GITDESCRIP" \
        -o ../../repospanner $@
)
(
    cd cmd/repobridge/
    go build -ldflags \
        "-X repospanner.org/repospanner/server/constants.version=$VERSION
        -X repospanner.org/repospanner/server/constants.gitdescrip=$GITDESCRIP" \
        -o ../../repobridge $@
)
(
    cd cmd/repohookrunner/
    go build -ldflags \
        "-X repospanner.org/repospanner/server/constants.version=$VERSION
        -X repospanner.org/repospanner/server/constants.gitdescrip=$GITDESCRIP" \
        -o ../../repohookrunner $@
)
