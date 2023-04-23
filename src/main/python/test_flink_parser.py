import os
import shutil
import unittest
import numpy as np

import pandas as pd
import torch
import flink_parser
import numpy as np
import rocksdb

class TestFlinkParser(unittest.TestCase):

    def setUp(self):
        # setup a test rocksdb
        opts = rocksdb.Options()
        opts.create_if_missing = True
        node_db = rocksdb.DB("node.db", opts)
        node_db.put('1'.encode('utf-8'), int(1).to_bytes(4, byteorder = 'big') + np.array([0.01, 0.02, 0.03, 0.04, 0.05]).tobytes())
        node_db.put('2'.encode('utf-8'), int(2).to_bytes(4, byteorder = 'big') + np.array([0.02, 0.03, 0.04, 0.05, 0.06]).tobytes())
        node_db.put('3'.encode('utf-8'), int(3).to_bytes(4, byteorder = 'big') + np.array([0.03, 0.04, 0.05, 0.06, 0.07]).tobytes())
        node_db.put('4'.encode('utf-8'), int(4).to_bytes(4, byteorder = 'big') + np.array([0.04, 0.05, 0.06, 0.07, 0.08]).tobytes())
        node_db.put('5'.encode('utf-8'), int(5).to_bytes(4, byteorder = 'big') + np.array([0.05, 0.06, 0.07, 0.08, 0.09]).tobytes())
        node_db.put('6'.encode('utf-8'), int(6).to_bytes(4, byteorder = 'big') + np.array([0.06, 0.07, 0.08, 0.09, 0.10]).tobytes())
        node_db.put('7'.encode('utf-8'), int(7).to_bytes(4, byteorder = 'big') + np.array([0.07, 0.08, 0.09, 0.10, 0.11]).tobytes())
        node_db.put('8'.encode('utf-8'), int(8).to_bytes(4, byteorder = 'big') + np.array([0.08, 0.09, 0.10, 0.11, 0.12]).tobytes())
        node_db.put('9'.encode('utf-8'), int(9).to_bytes(4, byteorder = 'big') + np.array([0.09, 0.10, 0.11, 0.12, 0.13]).tobytes())
        node_db.put('10'.encode('utf-8'), int(0).to_bytes(4, byteorder = 'big') + np.array([0.10, 0.11, 0.12, 0.13, 0.14]).tobytes())
        node_db.put('11'.encode('utf-8'), int(1).to_bytes(4, byteorder = 'big') + np.array([0.11, 0.12, 0.13, 0.14, 0.15]).tobytes())
        node_db.put('12'.encode('utf-8'), int(2).to_bytes(4, byteorder = 'big') + np.array([0.12, 0.13, 0.14, 0.15, 0.16]).tobytes())   
        node_db.put('13'.encode('utf-8'), int(3).to_bytes(4, byteorder = 'big') + np.array([0.13, 0.14, 0.15, 0.16, 0.17]).tobytes())
        node_db.put('14'.encode('utf-8'), int(4).to_bytes(4, byteorder = 'big') + np.array([0.14, 0.15, 0.16, 0.17, 0.18]).tobytes())
        node_db.put('15'.encode('utf-8'), int(5).to_bytes(4, byteorder = 'big') + np.array([0.15, 0.16, 0.17, 0.18, 0.19]).tobytes())
        del node_db
        
    def tearDown(self):
        # delete the test rocksdb
        folder_to_delete = "node.db"
        if os.path.exists(folder_to_delete):
            # Use shutil.rmtree to delete the folder and its contents
            shutil.rmtree(folder_to_delete)
        else:
            print(f'Folder "{folder_to_delete}" does not exist.')

    def test_flink_parser_setup(self):
        parser = flink_parser.FlinkStreamDataset("node.db")
        self.assertEqual(1, 1)

        data = pd.DataFrame({'src': [1, 1, 1], 'dst': [11, 13, 15]})
        labels, features, edge_list = parser.parse_record(data)
        expected_features = np.array([[0.01, 0.02, 0.03, 0.04, 0.05], [0.11, 0.12, 0.13, 0.14, 0.15], [0.13, 0.14, 0.15, 0.16, 0.17], [0.15, 0.16, 0.17, 0.18, 0.19]])
        print("labels", labels)
        print("features", features)
        print("edge_list", edge_list)
        self.assertEqual(labels, [(1, 1), (11, 1), (13, 3), (15, 5)])
        self.assertEqual(edge_list, [(1, 11), (1, 13), (1, 15)])
        self.assertTrue(np.array_equal(features, expected_features))
        


    



    