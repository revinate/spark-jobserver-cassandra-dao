#!/usr/bin/env bash
echo creating keyspace
until cqlsh cassandra -f create_keyspace.cql > /dev/null 2>&1; do sleep 1; done
