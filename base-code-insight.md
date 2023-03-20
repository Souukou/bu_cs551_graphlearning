#### Dataset

https://uoe-my.sharepoint.com/:u:/g/personal/s2121589_ed_ac_uk/Ee9Ye2ousIpKtCPIrMd9CIUB1fawjUYVq8XUTJJocerlXA?e=UebzlG

#### **Structure of the preprocessed dataset**

* `edges_dataframe.csv` this document contains edge connections information. More specifically, the first column is the sequence number, the second and third column is the node id.

* `feat_data.npy:` shape (221769, 602). 221769 nodes in total, every node has 602 features.

* `targets.npy` shape (221769, 1). For every node, 221769 nodes in total, they have one prediction result. This is a category data from 0 to 40.

#### **To run the demo**

1. Create a conda env. I use python 3.7

   `conda create -n cs551 python=3.7` Create a conda env

2. install specified version of dependency

   `pip install -r requirements.txt`

3. Install pytorch

   `pip install torch torchvision torchaudio`

4. Install `dgl`

   `pip install dgl-cu113==0.7.2 -f https://data.dgl.ai/wheels/repo.html`

   This step is vital. DO NOT directly install with pip, as it cannot find necessary dependency. And also it need to specify the version, as they use a method `dgl.dataloading.MultiLayerNeighborSampler` only appear in this version, and seems changed a name in later version.

5. Run the training in the root directory of the `online-gnn-training`

   `python train reddit pytorch test_eval.csv tsne_1 --eval 10 --batch_timestep 20 --epochs_offline 25 –cuda` 

Remember to run in the modified version provided in this pull request. I fixed some bugs to make it run.

#### **Troubleshoot**

* `/opt/dgl/src/array/cpu/./spmm_blocking_libxsmm.h:267: Failed to generate libxsmm kernel for the SpMM operation!`

  It is a problem due to lack of CPU instruction set. Change a computer.

* `MultiLayerNeighborSampler not find`

  You are not installing correct version of `dgl`. Follow above step 4 to install version 0.7.2. This dependency is not specified in requirements.txt so will not automatically install with `pip install -r`.

* `dgl._ffi.base.DGLError: [02:53:54] /opt/dgl/src/runtime/c_runtime_api.cc:88: Check failed: allow_missing: Device API gpu is not enabled.` 

  Please install the cuda version of `dgl`. You installed non-cuda version of dgl. Reinstall cuda version. (Or you installed dgl cuda version but is running the code pytorch-gpu)

#### **How this demo code work**

* the load will call the loading method from train/dataset_utils/reddit.py

  feat_data_size: the number of features in feat.npy. here is 602

  labels: all the labels in `targets.npy`. here is from 0 to 40

  graph: the graph constructed by `DynamicGraphEdge`. Here it use `--snapshots` to split the temporal graph into N snapshots. by default it is 100. They do the build of graph in `train/graph/dynamic_graph_edge.py`. There are `5914872` edges in the `edges_dataframe.csv`, and it uses 5000 snapshots. So in each snapshot, it contains 1182 edges. Then it only takes out the vertices in the first snapshots(idk why), and in the first snapshot, it only contains vertices from 0 to 512. Then it added the feats and targets of these 513 nodes to the `self.current_subgraph`. Finally it added two-way edges of all edges in the first snapshot. **->We need to do the same process in FLink part.**

  n_classes: type of labels in `targets.npy`, here is 41

  dynamic_graph_test: same as `graph` above

* After get the above `graph`, it will use it to construct `graph_util` from `TrainTestGraph` . Then the code will create four separate model(`graphsage_random`, `graphsage_priority`, `graphsage_no_reh`, `graphsage_full`), adapting different sampling method, evaluate and run them.

* Training. For each time step, it will call the `train_timestep` from `train/graphsage/model.py` to do one step of training. And inside the `train_timestep` it will use `choose_vertices` to do the sampling. e.g. `train/graphsage/pytorch/model.py`. **This sampling method is different for four models, as is also the part we need to replace.**

The other parts are not so important currently, as the professor said we do not need to modify the GNN part. We only need to replace the above said training part with a daemon that constantly read from a local file, then we can do the training part.  





 

