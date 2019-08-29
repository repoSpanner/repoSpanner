# repoSpanner Ansible roles

This directory contains three Ansible roles that are useful when deploying a repoSpanner cluster:

* ```ca``` - Used to configure the certificate authority, and to generate node certificates.
* ```install``` - Used to download the repoSpanner source code, build, and install.
* ```node``` - Used to configure the repoSpanner nodes and join them to the cluster.


## Role variables

The following variables can be used to customize your deployment:


### Build/install settings

repoSpanner is currently installed by downloading the source and compiling it. The following
settings pertain to customizing the build or installation.

* ```repospanner_prefix``` (str) - A path prefix to be used when installing repoSpanner to the
  nodes. Defaults to ```"/usr"```.
* ```repospanner_repo``` (str) - This is the git repository from which the sources should be pulled.
  Defaults to ```"https://github.com/repoSpanner/repoSpanner.git"```.
* ```repospanner_update``` (bool) - Whether or not to update the repospanner deployment on
  subsequent runs of the playbook. Defaults to ```true```.
* ```repospanner_version``` (str) - Which git ref to install. Defaults to ```"master"```.
* ```repospanner_build_deps``` (seq of str) - A list of build dependencies for building repoSpanner
  on each node. Defaults to ```["golang"]```.
* ```repospanner_clone_path``` (str) - A filesystem path in which to clone the repoSpanner sources.
  Defaults to ```"/tmp/repospanner"```.


### Configuration settings

The following settings pertain to configuring repoSpanner itself:

* ```repospanner_admin_address``` (str) - The address that the administrative interface
  listens on. Defaults to ```"0.0.0.0"```.
* ```repospanner_client_address``` (str) - The address that the client interface listens
  on. Defaults to ```"0.0.0.0"```.
* ```repospanner_admin_port``` (int) - The port that the administrative interface listens on.
  Defaults to ```8443```.
* ```repospanner_client_port``` (int) - The port that the client interface listens on. Defaults to
  ```443```.
* ```repospanner_cluster``` (str) - The top level domain name of the cluster. Defaults to
  ```"repospanner.example.com"```.
* ```repospanner_region_name``` (str) - The name of the region the node is part of. Defaults to
  ```"dc0"```.


### CA settings

The CA has one setting:

* ```repospanner_nodes``` (seq of str) - A list of node hostnames. The CA needs to know a list of
  the nodes in order to generate their client certificates and keys. This setting should simply be a
  list of node hostnames. It defaults to ```"{{ groups['repospanner_nodes'] }}"```), or more simply
  stated, the list of hosts in the Ansible group named ```"repospanner_nodes"```.


## Example playbook

Here is an example playbook that deploys a repoSpanner cluster. It assumes you have an Ansible
group called ```repospanner_nodes``` defined that container a list of hosts you want to act as nodes
in your cluster. It also assumes you have a host called repospanner_ca.example.com to act as your
CA.

```
---
- hosts:
    - repospanner_ca.example.com
    - repospanner_node
  become: true
  vars:
  roles:
    - repospanner_install


- hosts:
    - repospanner_ca.example.com
  become: true
  vars:
  roles:
    - repospanner_ca


- hosts:
    - repospanner_node
  become: true
  vars:
  roles:
    - repospanner_node
  tags:
    - node
```
