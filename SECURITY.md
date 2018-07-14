Security
========

This documents some of the security assumptions/goals that repoSpanner should adhere to, and provides some information on secure deployments.


Listeners
---------

repoSpanner has two sockets on which it accepts inbound connections: an RPC listener and a user/"http" listener, both requiring TLS and speaking http(/2).

The RPC endpoint is used for communication between different nodes and within a node: it always requires a TLS client certificate with the "repoSpanner node" key usage.
It has very low-level calls that could bring the system in an inconsistent state by performing invalid requests.
It can be firewalled so that only the other nodes and each node itself can communicate to it.

The user endpoint is used for requests by user clients (be that Git, repoclient, or any other client).
TLS client certificates are optional, although unauthenticated requests are only able to clone public repositories and get the high-level version information of the system.
This endpoint should be available from clients who need access to the repositories on this system.


Reporting a vulnerability
-------------------------

If you find a case in repoSpanner where it doesn't adhere to the expectations set out in this document (breakage of security expectations) or any other security vulnerabilities, please let us know and we will work with you to resolve the issue.
Please send an email to patrick@puiterwijk.org, and I will get back to you as soon as possible.
