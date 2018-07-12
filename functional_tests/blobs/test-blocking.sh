#!/bin/bash
echo "RUNNING HOOK"
echo "Args: $@"
echo "STDIN FOLLOWING"
cat </dev/stdin
echo "STDIN DONE"
echo "LS /"
ls /
echo "BLOCKING THE PUSH"
exit 1
