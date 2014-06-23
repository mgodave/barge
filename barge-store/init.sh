#!/bin/sh -vx
# send init to all registered members of the cluster

CONF=barge.conf

if [ $# -gt 0 ]; then
  echo "using $1 as configuration file..."
  CONF=$1
fi

if [ ! -r $CONF ]; then
  echo "configuration $CONF does not exist or not readable, giving up!"
  exit 1
fi

cat $CONF | tr "=" " " | while read index uri; do
    curl -X POST ${uri}raft/init
done 
