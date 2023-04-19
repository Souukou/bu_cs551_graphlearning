#!/bin/bash
echo "Generating Dataset"
python dump_to_rocksdb.py

echo "Compiling Protobufs"
JAVA_BASE="src/main/java/graphlearning"
protoc -I protobuf/proto --python_out=protobuf/ --java_out="${JAVA_BASE}/" protobuf/proto/event.proto

echo "Building..."
mvn -DskipTests -Drat.skip=true -Dcheckstyle.skip install

echo "Starting Cluster and Launching Job"
start-cluster.sh
flink run -pyfs src/main/python/ target/GraphOperators.jar --pyscript src/main/python/train.py --model-path /opt/trained_model.pth

echo "Trained"
read -p "Press enter to shutdown"

