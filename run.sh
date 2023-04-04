#!/bin/bash
echo "Generating Dataset"
python dump_to_rocksdb.py

echo "Building..."
pushd dl-on-flink/
mvn -DskipTests -Drat.skip=true -Dcheckstyle.skip install
popd
echo "Starting Cluster and Launching Job"
start-cluster.sh
flink run -pyfs graphlearning/ dl-on-flink/dl-on-flink-graphlearning/target/GraphOperators.jar --pyscript graphlearning/train.py --model-path /opt/trained_model.pth

echo "Trained"
read -p "Press enter to shutdown"

