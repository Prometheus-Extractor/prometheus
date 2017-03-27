#!/bin/bash

function testing() {
  if [ $1 == "eval" ]; then
    echo "eval"
    local cmd="eval command"
  fi
  echo "$cmd"
}
echo "all args: $@"

echo "all but first: ${@:2}"
PARAM=${PARAM:-default-with-dashes}
echo $PARAM

testing eval

