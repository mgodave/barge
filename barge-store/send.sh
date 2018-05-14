#!/bin/sh -vx

function usage () {
  cat <<EOF 
Usage: init.sh <uri> <Data>
Send some data to commmit to given server, in binary form
EOF
}

if [ $# -ne 2 ]; then
    usage
    exit 1
fi

URI=$1

while expr $URI : '.*/$' 2> /dev/null; do
    URI=$(expr $URI : '\(.*\)/')
done 

curl -X POST -H "Content-type:application/octet-stream" -d "$2" $URI/raft/commit
