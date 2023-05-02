# GNNs on Larger than Memory Streaming Graphs

**Design Document:** [Here](DESIGN.md)

***REMOVED***

## Setup
Install Docker [docker.io](docker.io). To train on GPU, download NVIDIA docker [here](https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/install-guide.html).

```bash
git clone https://cs551-gitlab.bu.edu/cs551/spring23/group-2/team-2.git
chmod 777 team-2 team-2/protobuf
cd team-2
```
#### Launching Host
Train on CPU
```bash
docker run -d --name host -v $PWD:/opt -p 6006:6006 -p 8081:8081 -p 9092:9092 captain0pool/streaming:deploy
```
For NVIDIA runtime:
```bash
docker run -d --name=host --runtime=nvidia -v $PWD:/opt -p 6006:6006 -p 8081:8081 -p 9092:9092 captain0pool/streaming:deploy
```

#### Running Code
```bash
docker exec -it host /opt/run.sh <path_to_pretrain_dict.npy> <path_to_pretrained_model.pth>
```

To use the pretrained file we provide, use

```bash
docker exec -it host /opt/run.sh pretrain_dict.pubmed.npy pretrained_graph_sage.pth

```
