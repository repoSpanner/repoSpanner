#!/bin/bash
echo "RUNNING HOOK"
echo "PS: $$"
echo "ID: `id`"
echo "Hostname: `hostname`"
env
echo "Args: $@"
echo "STDIN FOLLOWING"
cat </dev/stdin
echo "STDIN DONE"
echo "LS /"
ls /
