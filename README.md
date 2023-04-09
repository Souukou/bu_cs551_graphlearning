# GNNs on Larger than Memory Streaming Graphs

**Design Document:** [Here](DESIGN.md)

**Reddit Dataset
**: [Download Here](https://uoe-my.sharepoint.com/:u:/g/personal/s2121589_ed_ac_uk/Ee9Ye2ousIpKtCPIrMd9CIUB1fawjUYVq8XUTJJocerlXA?e=UebzlG)

## Setup

Install Miniconda [Download Here](https://docs.conda.io/en/latest/miniconda.html).

--------------
**If you are on M1 Mac**

```bash
CONDA_SUBDIR=osx-64 conda create -n streaming python=3.8
conda config --env --set subdir osx-64
```

--------------

```bash
conda create -n streaming python=3.8
conda install tensorflow -c tensorflow
conda install pytorch -c pytorch
conda install "dgl<0.8" -c dglteam
pip3 install seaborn pandas
pip3 install -r requirements.txt
```

## Running

### Build and run with prebuild environment

```bash
git clone https:://cs551-gitlab.bu.edu/cs551/spring23/group-2/team-2.git
cd team-2/
docker run -v $PWD:/opt -p 8081:8081 -it captain0pool/streaming:latest /opt/run.sh
````

### Build and run manually

WIP
