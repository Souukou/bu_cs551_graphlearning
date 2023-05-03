# GNNs on Larger than Memory Streaming Graphs

**Design Document:** [Here](DESIGN.md)

***REMOVED***

**Evaluations** [Here](Evaluations.md)

## Setup
Install Docker [docker.io](docker.io). To train on GPU, download NVIDIA docker [here](https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/install-guide.html).

```bash
git clone https://cs551-gitlab.bu.edu/cs551/spring23/group-2/team-2.git
chmod 777 team-2 team-2/protobuf
cd team-2
```
#### Launching Host
Run the docker with name `host`. If you already have a docker in the same name, remove it or change the following docker name to yours.

Train on CPU
```bash
docker run -d --name host -v $PWD:/opt -p 6006:6006 -p 8081:8081 -p 9092:9092 captain0pool/streaming:deploy
```
For NVIDIA runtime:
```bash
docker run -d --name=host --runtime=nvidia -v $PWD:/opt -p 6006:6006 -p 8081:8081 -p 9092:9092 captain0pool/streaming:deploy
```

#### Running Code

Enter the docker

```bash
docker exec -it host /bin/bash
```

In the docker, run the command

```bash
/opt/run.sh <path_to_pretrain_dict.npy> <path_to_pretrained_model>
```

To use the pretrained file we provide, use

```bash
/opt/run.sh models/pretrain_dict.pubmed.npy models/pretrained_graph_sage.pth

```

**Note**

After running the command, you can go to `localhost:8081` and check the stdout of the task manager. Ignore the `UnsupportedOperationException` in the trace information, as it is a problem related to RocksDB. You will then be able to view the losses of the training. TensorBoard monitoring can be accessed at `localhost:6006`. You may need to wait for a while until it becomes available.

#### Running the Unit Test

The major components are covered by JUnit. You can run the tests with the following command:

```
mvn test
```