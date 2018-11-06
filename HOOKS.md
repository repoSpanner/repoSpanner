Hooks
=====

repoSpanner supports plain Git hooks, which can be uploaded via a call to
the editRepo call.
The script will be uploaded, and run on the repoSpanner node that processes
the push.
There are some differences in hooks running inside repoSpanner as compared
to standard Git hooks, but in general any existing hook should work as-is,
as long as they don't try to write to the repository themselves.


Run environment
---------------

Hooks are executed in a temporary, bare, Git clone of the repository.
This means that any changes they apply to objects or references in the
repository will NOT persist.
Depending on the configuration of repoSpanner, the hooks might be executed
in a bubblewrap environment, in which they only have the specific mounts
the administrator grants them.
