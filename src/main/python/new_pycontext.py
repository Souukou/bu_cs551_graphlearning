from io import StringIO

import numpy as np
import pandas as pd
import torch
from dl_on_flink_framework.context import Context
from dl_on_flink_pytorch.flink_stream_dataset import (
    DL_ON_FLINK_TYPE_TO_PYTORCH_TYPE,
    FlinkStreamDataset,
)
from dl_on_flink_pytorch.pytorch_context import PyTorchContext
from torch_geometric.data import Data as PyG_Data

import rocksdb
from typing import Tuple

class NewPyTorchContext(PyTorchContext):
    def get_dataset_from_flink(self, nodeDb = "dataset-test/node.db") -> FlinkStreamDataset:
        """
        Get the data loader to read data from Flink.
        """
        return NewFlinkStreamDataset(self, nodeDb)

class NewFlinkStreamDataset(FlinkStreamDataset):
    def __init__(self, context: Context, nodeDb = "dataset-test/node.db"):
        super(NewFlinkStreamDataset, self).__init__(context)
        opts = rocksdb.Options()
        self.node_db = rocksdb.DB(nodeDb, opts, read_only=True)

    def read_label_and_features_rocksdb(self, node_id: int):
        value = self.node_db.get(str(node_id).encode("utf-8"))
        # print(value)
        if value is None:
            print("getting None value for node", str(node_id))
            return None, None
        label = int.from_bytes(value[:4], byteorder="big")
        print(node_id,label, len(value))
        print(len(value[4:]))
        features = torch.tensor(np.frombuffer(value[4:], np.dtype(int)))
        print(len(features))
        return label, features

    def get_all_node_feats(self, edges: list):
        all_nodes = set()
        for node in edges[0]:
            all_nodes.add(node)

        all_node_list = list(all_nodes)
        all_node_list.sort()

        labels = []
        features = []
        idn = 0
        for node in all_node_list:
            idn += 1
            print("getting the node:", node)
            label, feature = self.read_label_and_features_rocksdb(node)
            labels.append((node, label))
            # features.append(feature)
            features.append([idn, node, label])
        features = torch.stack(features)
        return labels, features, all_node_list
    
    def create_input(self, src, edges, labels, features, all_node_list):
        
        idx_map = {src.item():0}
        for idn in range(len(all_node_list)):
            idx_map[all_node_list[idn]] = idn + 1
        new_edges = torch.zeros((2, len(edges[0]))).long()
        for idn in range(len(edges[0])):
            new_edges[0][idn] = idx_map[edges[0][idn]]
            new_edges[1][idn] = idx_map[edges[1][idn]]
        mask_all = torch.tensor([0]).long()
        new_data = PyG_Data(x = features, edge_index = new_edges, y = labels, train_mask = mask_all, val_mask = mask_all, test_mask = mask_all)
        return new_data
    
    def decode_edge(self, edge_str):
        edge_strs = edge_str.split('|')
        edges = [[],[]]
        for estr in edge_strs:
            node1, node2 = list(map(int, estr.split('-')))
            edges[0] += [node1, node2]
            edges[1] += [node2, node1]
        return edges
    
    def parse_record(self, record):
        with open('/opt/res.txt', 'w') as f:
          f.write(record)

        df = pd.read_csv(StringIO(record), names=["src", "edges"], encoding='utf8')
        print(record)
        df.to_csv('/opt/res.csv') 
        #df['embed'] = df['embed'].apply(lambda x: bytes.fromhex(x))
        ############ For debugging only, remove this later ######################
        # print("we are reading the sample data from the file")
        df = pd.read_csv('/opt/graphlearning/sample_data.csv')
        self.input_types = ["INT_64", "INT_64", "INT_64", "FLOAT_32"]
        print(df.columns)
        ############ For debugging only, remove this later ######################

        for idx, key in enumerate(["src", "edges"]):
            if key == 'src':
                src = df[key][0]
                # src = torch.from_numpy(np.array([df[key][0]])).to(DL_ON_FLINK_TYPE_TO_PYTORCH_TYPE[self.input_types[idx]])
            elif key == 'edges':
                edges = self.decode_edge(df[key][0])
                # edges = torch.from_numpy(edges).to(DL_ON_FLINK_TYPE_TO_PYTORCH_TYPE[self.input_types[idx]])
        labels, features, all_node_list = self.get_all_node_feats(edges)
        labels = torch.from_numpy(edges).to(DL_ON_FLINK_TYPE_TO_PYTORCH_TYPE[self.input_types[2]])
        features = torch.from_numpy(edges).to(DL_ON_FLINK_TYPE_TO_PYTORCH_TYPE[self.input_types[3]])
        # all_node_list = torch.from_numpy(edges).to(DL_ON_FLINK_TYPE_TO_PYTORCH_TYPE[self.input_types[0]])
        pyg_data = self.create_input(src, edges, labels, features, all_node_list)
        print("pyg data", pyg_data)
        print("pyg data feature", pyg_data.x)
        print("pyg data edges", pyg_data.edge_index)
        print("pyg data label", pyg_data.y)
        
        return pyg_data
