#!/bin/sh -xe
if [ ! -d vendor/github.com ];
then
	dep ensure
fi

if [ "$FULLUPDATE" = "yes" ];
then
	dep ensure -v -update
	git diff Gopkg.lock
fi

export VERSION="`cat VERSION`"
export GITDESCRIP="`git describe --long --tags --dirty --always`"

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
