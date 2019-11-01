# Contribution guide

Most changes do not require special tools other than the standard build
instructions, however, if you modify any of the protobuf files, you'll need to
install the protobuf compiler:

    $ dnf install protobuf-devel

Then run `go generate ./...` to regenerate the built files.

repoSpanner has an optional inbuilt profiler that can be enabled at compile
time. If you wish to use it, compile repoSpanner with the prof tag, like this:

	$ ./build.sh -tags prof

When repoSpanner starts, it will print a log message telling you how you
can access the profiling data, for example:

	RUNNING PROFILING ON  0.0.0.0:8444

You can use `pprof` to interpret the data:

	go tool pprof http://dev.example.com:8444

## Tests

The project comes with a decent functional test suite.  Explore the
`repospanner/functional_tests` to see the variety of tests that you can
run.

To run the full test suite, run from the main directory:

    $ go test ./...

### The devel folder

The [devel](devel/) folder contains some scripts that you can use to aid in development of
repoSpanner. The `build.sh` script uses podman to build a container that can be used to run the
test suite. `test.sh` is used to run the tests in the container that results from running
`build.sh`.

## Contributions

Contributions are most welcome. To send a patch, send a pull request to the project on github.
Please make sure to add a `Signed-Off-By` line in your git commit to indicate
you agree to the Developer Certificate of Origin (DCO) as quoted below.
To do this, simple add the "-s" flag to your git commit, like: `git commit -s`.

### Developer Certificate of Origin

Developer Certificate of Origin
Version 1.1

Copyright (C) 2004, 2006 The Linux Foundation and its contributors.
1 Letterman Drive
Suite D4700
San Francisco, CA, 94129

Everyone is permitted to copy and distribute verbatim copies of this
license document, but changing it is not allowed.

Developer's Certificate of Origin 1.1

By making a contribution to this project, I certify that:

(a) The contribution was created in whole or in part by me and I
    have the right to submit it under the open source license
    indicated in the file; or

(b) The contribution is based upon previous work that, to the best
    of my knowledge, is covered under an appropriate open source
    license and I have the right under that license to submit that
    work with modifications, whether created in whole or in part
    by me, under the same open source license (unless I am
    permitted to submit under a different license), as indicated
    in the file; or

(c) The contribution was provided directly to me by some other
    person who certified (a), (b) or (c) and I have not modified
    it.

(d) I understand and agree that this project and the contribution
    are public and that a record of the contribution (including all
    personal information I submit with it, including my sign-off) is
    maintained indefinitely and may be redistributed consistent with
    this project or the open source license(s) involved.
