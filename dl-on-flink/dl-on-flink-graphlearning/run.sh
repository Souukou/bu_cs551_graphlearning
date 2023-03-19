#!/bin/bash

cd dl-on-flink/dl-on-flink-graphlearning
mvn -DskipTests -Drat.skip=true install
start-cluster.sh
flink run -pyfs graphlearning/ dl-on-flink/dl-on-flink-graphlearning/target/GraphOperator.jar --pyscript graphlearning/main.py
