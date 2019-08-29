# Install by hand

## Build

Make sure you have a Go toolchain available, and run:

    $ ./build.sh

## Configuration

A repoSpanner deployment is called a "cluster", which consists of one or
more "regions", which contain one or more "nodes".  A cluster contains
all nodes that are under the same deployment, and a region contains all
nodes that talk and synchronize amongst each other.

The `nodename.regionname.clustername` should be the FQDNs the nodes use
to communicate with their peers.

You will need to do a small amount of configuration to get started.
Copy ```config.yml.example``` to ```/etc/repospanner/config.yml``` and
edit a few of the settings to match your environment:

* ```admin.url```
* ```certificates.client.cert```
* ```certificates.client.key```
* ```certificates.server.default.cert```
* ```certificates.server.default.key```

Of course, feel free to stroll through the file and season to taste as
well.


## Certificate authority

The repoSpanner binary contains all the tools needed to create the
Certificate Authority (CA) to perform the pushes.  To initiate a
cluster, run: `repospanner ca init <cluster-name>`.  E.g.:

    $ repospanner ca init repospanner.local.

To create node certificates, run: `repospanner ca node <region>
<nodename>`.  E.g.:

    $ repospanner ca node regiona nodea


# Join nodes to the cluster

After creating the certificates, deploy them to the nodes, and create
configuration files (default: `/etc/repospanner/config.yml`).  Then on
the first node, invoke the following to make it initialize its databases:

    $ repospanner serve --spawn

And then, to run it:

    $ repospanner serve

Or start the `repospanner.service` unit file.  Then on any further nodes,
run: `repospanner serve --join https://<running.node.fqdn>:<rpcport>`, e.g.:

    $ repospanner serve --joinnode \
        https://nodea.regiona.repospanner.local:8443

And then run:

    repospanner serve

Or, again, start the `repospanner.service` unit file.
