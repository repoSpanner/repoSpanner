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

export VERSION="$(cat VERSION)"
export GITDESCRIP="$(git describe --long --tags --dirty --always)"
export VENDORDESCRIP="$(find vendor -type f | xargs cat | sha1sum | tr -d '\n' | sed -e 's/  -//')"

(
    cd cmd/repospanner/
    go build -ldflags \
        "-X repospanner.org/repospanner/server/constants.version=$VERSION
        -X repospanner.org/repospanner/server/constants.gitdescrip=$GITDESCRIP
        -X repospanner.org/repospanner/server/constants.vendordescrip=$VENDORDESCRIP" \
        -o ../../repospanner $@
)
(
    cd cmd/repobridge/
    go build -ldflags \
        "-X repospanner.org/repospanner/server/constants.version=$VERSION
        -X repospanner.org/repospanner/server/constants.gitdescrip=$GITDESCRIP
        -X repospanner.org/repospanner/server/constants.vendordescrip=$VENDORDESCRIP" \
        -o ../../repobridge $@
)
(
    cd cmd/repohookrunner/
    go build -ldflags \
        "-X repospanner.org/repospanner/server/constants.version=$VERSION
        -X repospanner.org/repospanner/server/constants.gitdescrip=$GITDESCRIP
        -X repospanner.org/repospanner/server/constants.vendordescrip=$VENDORDESCRIP" \
        -o ../../repohookrunner $@
)
