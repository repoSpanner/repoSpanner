# repoSpanner

repoSpanner is a distributed repository storage server, based around Git.

It is designed so that all nodes are equal -- any client can push/pull
to/from any node and should see no difference at all.  Each node makes
sure that all pushed objects are synced to at least a majority of the
nodes before acknowledging the push.

The system should be resilient against any nodes failing, as long as a
majority of nodes remains available; with the worst case being a single
push failing due to an attempt to push to the failed node.

*Note*: As a consequence of this, it is strongly suggested to deploy
regions with odd numbers of nodes.


## Deployment

There are two ways to deploy repoSpanner:

* [Ansible](ansible/README.md)
* [Manual](INSTALL.md)


## Repository access

After the nodes are installed and running, the service will be available on https://<node.fqdn>/.


### Client certificates

To create leaf certificates (for admin and to push/pull), run:

    $ repospanner ca leaf <username>

And then arguments for whether the user is an admin (--admin), can pull
(--read), and/or push (--write); and for which regions and repositories
this certificate is valid (globbing possible), e.g.:

    $ repospanner ca leaf admin --admin --write \
        --read --region "*" --repo "*"


### Create

You can create repositories with ```repospanner admin repo create <name>```, for
example:

```
$ repospanner admin repo create repospanner
```

### Clone

For git repo pull/push, add a /repo/<repo-name>.git.
Example clone command for default https port on tcp/443 and repo name being "test"

```
git clone \
--config http.sslcert=/etc/pki/repospanner/someuser.crt \
--config http.sslkey=/etc/pki/repospanner/someuser.key \
--config http.sslCAinfo=/etc/pki/repospanner/<repospanner-CA>.crt \
https://nodea.regiona.repospanner.local/repo/test.git".

```

Alternatively, for ssh based pushing and pulling, make sure that the users'
entry console is the `repobridge` binary, and the client_config.yml file is setup
in /etc/repospanner.
This client will automatically revert to plain git if it determines the repo
that is being pushed to is not a repospanner repository.

## Development

For development, standard github pull requests are used.
Most changes do not require special tools other than the standard build
instructions, however, if you modify any of the protobuf files, you'll need to
install the protobuf compiler:

    $ dnf install protobuf-devel

Then run `go generate ./...` to regenerate the built files.

To run the full test suite, run from the main directory:

    $ go test ./...

## Tests

The project comes with a decent functional test suite.  Explore the
`repospanner/functional_tests` to see the variety of tests that you can
run.

## Contributions

Contributions are most welcome.
Please make sure to add a `Signed-Off-By` line in your git commit to indicate
you agree to the Developer Certificate of Origin (DCO) as quoted below.
To do this, simple add the "-s" flag to your git commit, like: `git commit -s`.

## Developer Certificate of Origin

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
