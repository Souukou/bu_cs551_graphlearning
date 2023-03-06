# GNNs on Larger than Memory Streaming Graphs

**Design Document:** [Here](DESIGN.md)

***REMOVED***
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
