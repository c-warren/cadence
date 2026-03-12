#!/bin/bash

until make install-schema-xdc; do sleep 1; done

echo "Stand up the binaries now!"

read -p "Press Enter to continue..."

./cadence --transport grpc --ad localhost:7833 --domain dlq-test domain register --gd true --clusters cluster0,cluster1,cluster2

echo "Domain registered, waiting 60 seconds"

sleep 60

./cadence --transport grpc --ad localhost:7833 --domain dlq-test domain update --active_clusters 'location.london:cluster0'

echo "Domain updated"

sleep 2

echo "Done\!"

# ./dlq-load-test --domain dlq-test --count 10 --worker-delay 1m
