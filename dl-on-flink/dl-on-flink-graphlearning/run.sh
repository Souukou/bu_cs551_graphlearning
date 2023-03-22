#!/bin/bash

pushd dl-on-flink/
mvn -DskipTests -Drat.skip=true -Dcheckstyle.skip install
popd
start-cluster.sh
flink run -pyfs graphlearning/ dl-on-flink/dl-on-flink-graphlearning/target/GraphOperators.jar --pyscript graphlearning/train.py
