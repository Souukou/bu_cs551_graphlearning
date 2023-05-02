from io import StringIO

import struct
from typing import List, Mapping

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
    def get_dataset_from_flink(self) -> FlinkStreamDataset:
        """
        Get the data loader to read data from Flink.
        """
        # print("getting the node db", nodeDb)
        return NewFlinkStreamDataset(self)

class NewFlinkStreamDataset(FlinkStreamDataset):
    def __init__(self, context: Context):
        super(NewFlinkStreamDataset, self).__init__(context)
        opts = rocksdb.Options()
        # print("rocksdb opts", opts)
        dataset_path = context.get_property("dataset_path") + "/nodes.db"
        self.node_db = rocksdb.DB(dataset_path, opts, read_only=True)
        self.pyg_data = None
        self.nidx = 0

    def read_label_and_features_rocksdb(self, node_id: int):
        value = self.node_db.get(str(node_id).encode("utf-8"))
        # print(value)
        if value is None:
            print("getting None value for node", str(node_id))
            return None, None
        label = int.from_bytes(value[:4], byteorder="big")
        features = torch.tensor(np.frombuffer(value[4:], np.dtype(np.float32)))
        return label, features

    def get_all_node_feats(self, edges: list):
        all_nodes = set()
        for node in edges[0]:
            all_nodes.add(node)

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
            label, feature = self.read_label_and_features_rocksdb(node)
            labels.append(torch.tensor([label]))
            features.append(feature)
        features = torch.stack(features)
        labels = torch.stack(labels)
        return labels, features, all_node_list
    
    def get_all_node_feats_proxy(self, edges: list):
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
            label, feature = self.dataset_proxy.y[node], self.dataset_proxy.x[node]
            labels.append(label)
            features.append(feature)
        features = torch.stack(features)
        return labels, features, all_node_list
    
    def create_input(self, src, edges, labels, features, all_node_list):
        # print("src", src)
        # print("edges:", edges)
        idx_map = {}
        for idn in range(len(all_node_list)):
            idx_map[all_node_list[idn]] = idn
        new_edges = torch.zeros((2, len(edges[0]))).long()
        # print("idx_map", idx_map)
        for idn in range(len(edges[0])):
            new_edges[0][idn] = idx_map[edges[0][idn]]
            new_edges[1][idn] = idx_map[edges[1][idn]]
        mask_all = [False for _ in range(len(all_node_list))]
        src_new_idx = idx_map[src.item()]
        mask_all[src_new_idx] = True
        
        new_data = PyG_Data(
            x=features,
            edge_index=new_edges,
            y=labels,
            train_mask=mask_all,
            val_mask=mask_all,
            test_mask=mask_all,
        )
        
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
        self.nidx += 1
        with open('/opt/res.txt', 'w') as f:
          f.write(record)
        # print("nidx", self.nidx)
        df = pd.read_csv(StringIO(record), names=["src", "edges"], encoding='utf8')
        self.input_types = ["INT_64", "INT_64", "INT_64", "FLOAT_32"]
        for idx, key in enumerate(["src", "edges"]):
            if key == 'src':
                src = df[key][0]
            elif key == 'edges':
                edges = self.decode_edge(df[key][0])
        labels, features, all_node_list = self.get_all_node_feats(edges)
        labels = labels.to(DL_ON_FLINK_TYPE_TO_PYTORCH_TYPE[self.input_types[2]])
        features = features.to(DL_ON_FLINK_TYPE_TO_PYTORCH_TYPE[self.input_types[3]])
        pyg_data = self.create_input(src, edges, labels, features, all_node_list)
        # print(pyg_data.edge_index)
        # print(pyg_data.x)
        # print(pyg_data.y)
        # print(pyg_data.train_mask)

        return pyg_data
        
        
