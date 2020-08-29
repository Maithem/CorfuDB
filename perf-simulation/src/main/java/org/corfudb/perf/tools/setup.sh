#!/bin/bash

rm -rf n0/* n1/* n2/*

nohup ./bin/corfu_server -l n0 9000 -N --Threads 4 --logunit-threads 4 2>&1 &
nohup ./bin/corfu_server -l n1 9001 -N --Threads 4 --logunit-threads 4 2>&1 &
nohup ./bin/corfu_server -l n2 9002 -N --Threads 4 --logunit-threads 4 2>&1 &

java -jar perf-simulation/target/form-cluster.jar -nodes localhost:9000,localhost:9001,localhost:9002
