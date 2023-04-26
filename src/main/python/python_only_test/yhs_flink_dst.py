import os
import shutil
import unittest
import numpy as np

import pandas as pd
import torch

import rocksdb
from typing import Tuple

class TestFlinkParser:

    def setUp(self):
        pass
        # setup a test rocksdb
        # opts = rocksdb.Options()
        # opts.create_if_missing = True
        # node_db = rocksdb.DB("/opt/graphlearning/example/node.db", opts)
        # node_db.put('1'.encode('utf-8'), int(1).to_bytes(4, byteorder = 'big') + '0.01,0.02,0.03,0.04,0.05'.encode('utf-8'))
        # node_db.put('2'.encode('utf-8'), int(2).to_bytes(4, byteorder = 'big') + '0.02,0.03,0.04,0.05,0.06'.encode('utf-8'))
        # node_db.put('3'.encode('utf-8'), int(3).to_bytes(4, byteorder = 'big') + '0.03,0.04,0.05,0.06,0.07'.encode('utf-8'))
        # node_db.put('4'.encode('utf-8'), int(4).to_bytes(4, byteorder = 'big') + '0.04,0.05,0.06,0.07,0.08'.encode('utf-8'))
        # node_db.put('5'.encode('utf-8'), int(5).to_bytes(4, byteorder = 'big') + '0.05,0.06,0.07,0.08,0.09'.encode('utf-8'))
        # node_db.put('6'.encode('utf-8'), int(6).to_bytes(4, byteorder = 'big') + '0.06,0.07,0.08,0.09,0.10'.encode('utf-8'))
        # node_db.put('7'.encode('utf-8'), int(7).to_bytes(4, byteorder = 'big') + '0.07,0.08,0.09,0.10,0.11'.encode('utf-8'))
        # node_db.put('8'.encode('utf-8'), int(8).to_bytes(4, byteorder = 'big') + '0.08,0.09,0.10,0.11,0.12'.encode('utf-8'))
        # node_db.put('9'.encode('utf-8'), int(9).to_bytes(4, byteorder = 'big') + '0.09,0.10,0.11,0.12,0.13'.encode('utf-8'))
        # node_db.put('10'.encode('utf-8'), int(0).to_bytes(4, byteorder = 'big') + '0.10,0.11,0.12,0.13,0.14'.encode('utf-8'))
        # node_db.put('11'.encode('utf-8'), int(1).to_bytes(4, byteorder = 'big') + '0.11,0.12,0.13,0.14,0.15'.encode('utf-8'))
        # node_db.put('12'.encode('utf-8'), int(2).to_bytes(4, byteorder = 'big') + '0.12,0.13,0.14,0.15,0.16'.encode('utf-8'))
        # node_db.put('13'.encode('utf-8'), int(3).to_bytes(4, byteorder = 'big') + '0.13,0.14,0.15,0.16,0.17'.encode('utf-8'))
        # node_db.put('14'.encode('utf-8'), int(4).to_bytes(4, byteorder = 'big') + '0.14,0.15,0.16,0.17,0.18'.encode('utf-8'))
        # node_db.put('15'.encode('utf-8'), int(5).to_bytes(4, byteorder = 'big') + '0.15,0.16,0.17,0.18,0.19'.encode('utf-8'))
        # del node_db
        
    def tearDown(self):

        pass
        # delete the test rocksdb
        # folder_to_delete = "/opt/graphlearning/example/node.db"
        # if os.path.exists(folder_to_delete):
        #     # Use shutil.rmtree to delete the folder and its contents
        #     shutil.rmtree(folder_to_delete)
        # else:
        #     print(f'Folder "{folder_to_delete}" does not exist.')

    def test_flink_parser_setup(self, edges):
        # /home/grad3/amliu/team-2/
        parser = FlinkStreamDataset("/opt/dataset-test/nodes.db")
        # self.assertEqual(1, 1)
        labels, features, all_node_list = parser.parse_record(edges)
        for label, feat, node in zip(labels, features, all_node_list):
            # print(label, feat, node)
            print("label shape", label)
            print("feat shape", feat.shape)


class FlinkStreamDataset:
    def __init__(self, nodeDb="/opt/dataset-test/nodes.db") -> None:
        opts = rocksdb.Options()
        self.node_db = rocksdb.DB(nodeDb, opts, read_only=True)

    def read_label_and_features_rocksdb(self, node_id: int) -> Tuple[int, torch.tensor]:
        value = self.node_db.get(str(node_id).encode("utf-8"))
        # print(value)
        if value is None:
            print("getting None value for node", str(node_id))
            return None, None
        label = int.from_bytes(value[:4], byteorder="big")
        print(node_id,label, len(value))
        print(len(value[4:]))
        features = torch.tensor(np.frombuffer(value[4:]))
        print(len(features))
        return label, features

    def parse_record(self, edges: list):
        all_nodes = set()
        for node in edges[0]:
            all_nodes.add(node)

        all_node_list = list(all_nodes)
        all_node_list.sort()

        labels = []
        features = []
        for node in all_node_list:
            print("getting the node:", node)
            label, feature = self.read_label_and_features_rocksdb(node)
            labels.append(label)
            features.append(feature)
        features = torch.stack(features)
        return labels, features, all_node_list
