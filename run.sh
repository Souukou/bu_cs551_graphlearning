#!/bin/bash

if [[ -z $1 ]] || [[ -z $2 ]]; then
  echo "Requires path to pretrain_dict.npy. Usage: $0 <w.r.t. project_root>/relative/path/to/pretrain_dict.npy <w.r.t project_root>/relative/path/to/trained_model_graphsage.pth"
  exit 1
fi

rm -rf /opt/data/pubmed
mkdir -p /opt/data/tmp

MODEL="${2%.*}"
echo "Saving trained model to data/tmp/model.pth"

echo "creating pretrained_nodes.json for java ..."
python /opt/utils/convert.py --numpy $1 --json /opt/data/pretrained_nodes.json

echo "Building project ..."
mvn -DskipTests -Drat.skip=true -Dcheckstyle.skip install

echo "Downloading and Dumping Dataset ..."
python /opt/dumper.py -p /opt/$1 -s /opt/data/pubmed -m train

echo "Publishing untrained data to Kafka Stream"

python /opt/dumper.py -p /opt/$1 -k -m train -t train

echo "Launching Tensorboard on => localhost:6006"
tensorboard --logdir /opt/"$MODEL""_tsb" --host 0.0.0.0 --port 6006 &> /opt/data/tmp/tb.log &

echo "Starting Cluster and Launching Job"
start-cluster.sh

echo "Flink Dashboard up on localhost:8081"
echo "Submitting Job"
flink run -pyfs /opt/src/main/python/ /opt/target/GraphOperators.jar --pyscript /opt/src/main/python/train_online_val.py --model-path $MODEL --mode train --properties /opt/config/prop.config
