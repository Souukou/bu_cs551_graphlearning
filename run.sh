#!/bin/bash

if [[ -z $1 ]] || [[ -z $2 ]] then;
  echo "requires path to pretrain_dict.npy. Usage: $0 <w.r.t. project_root>/relative/path/to/pretrain_dict.npy <w.r.t project_root>/relative/path/to/pretrained_model_graphsage.pth"
  exit 1
fi

rm -rf pubmed/
mkdir -p tmp/

MODEL="${2%.*}"
echo "Saving trained model to tmp/model.pth"

echo "creating pretrained_nodes.json for java ..."
python utils/convert.py -n $1 --json /opt/src/main/java/graphlearning/sampling/pretrained_nodes.json

echo "Building project ..."
mvn -DskipTests -Drat.skip=true -Dcheckstyle.skip install

echo "Downloading and Dumping Dataset ..."
python dumper.py -p /opt/$1 -s pubmed -m train

echo "Publishing untrained data to Kafka Stream"

python dumper.py -p /opt/$1 -k -m train -t train

echo "Launching Tensorboard on => localhost:6006"
tensorboard --logdir "$MODEL""_tsb" --host 0.0.0.0 --port 6006 &> /tmp/tb.log &

echo "Starting Cluster and Launching Job"
start-cluster.sh

echo "Flink Dashboard up on localhost:8081"
echo "Submitting Job"
flink run -pyfs /opt/src/main/python/ /opt/target/GraphOperators.jar --pyscript /opt/src/main/python/train_online_val.py --model-path $MODEL --mode train --properties /opt/prop.config
