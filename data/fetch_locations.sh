#!/bin/bash
SNAPSHOT=1656292357
# SNAPSHOT=latest

# get last snapshot
curl https://bitnodes.io/api/v1/snapshots/$SNAPSHOT/ \
  | jq -r '.nodes | .[] | .[8:10] | select((.[0] != 0) or (.[1] != 0)) | @csv' \
  > bitcoin_nodes.csv

