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


## Operating Repospanner cluster
### TLS/x509 certificates renewal
As repospanner uses TLS between all components (server to server, client to server, etc), it's important that you check the default validity of those certificates.
You can use basic openssl command to check default validity (1y per default):
```
openssl x509 -in node1.cluster1.crt -noout -text|grep -A 3 Validity
        Validity
            Not Before: Sep 17 13:43:13 2018 GMT
            Not After : Sep 17 13:43:13 2019 GMT
        Subject: CN=node1.cluster1.domain.org
```

If you have to renew, you'll have to recreate certififcates with same CN but also exact same NodeID (used in repospanner).
First you have to find the NodeID for that node :

```
repospanner ca info /path/to/node1.cluster1.crt 
Certificate information:
Subject: node1.cluster1.domain.org
Certificate type:  Node
repoSpanner cluster name: domain.org
repoSpanner region name: cluster1
repoSpanner Node Name: node1
repoSpanner Node ID: 2
```

As we have now identified that Node ID is two, we can just issue a new key/crt for that node (you'll have to delete existing .crt/.key file first otherwise repospanner would complain that those already exist with ```panic: Node certificate exists```) :

```
repospanner ca node --nodeid 2 --years 30 cluster1 node1
Done

```
You can now replace on your node and restart repospanner instance

## Contribute

If you would like to get involved in repoSpanner development, take a look at the
[Contribution guide](devel/CONTRIBUTE.md).
