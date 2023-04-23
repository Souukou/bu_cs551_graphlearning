import pandas as pd
import rocksdb
import torch
from typing import Tuple
import numpy as np


class FlinkStreamDataset:
    def __init__(self, nodeDb="dataset-test/node.db") -> None:
        opts = rocksdb.Options()
        self.node_db = rocksdb.DB(nodeDb, opts, read_only=True)

    def read_label_and_features_rocksdb(self, node_id: int) -> Tuple[int, torch.tensor]:
        value = self.node_db.get(str(node_id).encode("utf-8"))
        label = int.from_bytes(value[:4], byteorder="big")
        features = torch.tensor(np.frombuffer(value[4:]))
        return label, features

    def parse_record(self, data: pd.DataFrame):
        edge_list = []
        all_nodes = set()
        for index, row in data.iterrows():
            src = row["src"]
            dst = row["dst"]
            all_nodes.add(src)
            all_nodes.add(dst)
            edge_list.append((src, dst))

        all_node_list = list(all_nodes)
        all_node_list.sort()

        labels = []
        features = []
        for node in all_node_list:
            label, feature = self.read_label_and_features_rocksdb(node)
            labels.append((node, label))
            features.append(feature)
        features = torch.stack(features)
        return labels, features, edge_list
