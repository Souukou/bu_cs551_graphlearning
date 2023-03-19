#!/bin/bash

pushd dl-on-flink/
mvn -DskipTests -Drat.skip=true -Dchecktyle.skip install
popd
start-cluster.sh
flink run -pyfs graphlearning/ dl-on-flink/dl-on-flink-graphlearning/target/GraphOperator.jar --pyscript graphlearning/main.py
