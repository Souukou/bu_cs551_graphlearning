import torch
import numpy as np
import pandas as pd
from dl_on_flink_framework.context import Context
from dl_on_flink_pytorch.pytorch_context import PyTorchContext
from dl_on_flink_pytorch.flink_stream_dataset import FlinkStreamDataset, DL_ON_FLINK_TYPE_TO_PYTORCH_TYPE
from io import StringIO
from torch_geometric.data import Data as PyG_Data

class NewPyTorchContext(PyTorchContext):
    def get_dataset_from_flink(self) -> FlinkStreamDataset:
        """
        Get the data loader to read data from Flink.
        """
        return NewFlinkStreamDataset(self)

class NewFlinkStreamDataset(FlinkStreamDataset):
    def __init__(self, context: Context):
        super(NewFlinkStreamDataset, self).__init__(context)
        # get the byte length here
        # self.byte_len = int(self.pytorch_context.get_property('BYTE_LEN'))

    def decoding_features(self, feats_str):
        print("feats_str:", feats_str.__class__)
        print(("length and node num",len(feats_str), self.node_num))
        assert len(feats_str) % self.node_num == 0
        self.byte_len = len(feats_str) // self.node_num
        feat_strs = [feats_str[i * self.byte_len : (i+1) * self.byte_len] for i in range(self.node_num)]
        # modify this to decode the feat
        feats = [np.frombuffer(feat_str, np.dtype(int)) for feat_str in feat_strs]
        return feats
    def decoding_neighbors(self, feats_str):
        if isinstance(feats_str, np.int64):
            neighbor_ls = [feats_str.item()]
        else:
            feat_str_ls = feats_str.split('-')
            neighbor_ls = list(map(int, feat_str_ls))
            # neighbor_ls = map(int, feats_str.split(','))
            # print(feat_str_ls, neighbor_ls)
        self.node_num = len(neighbor_ls) + 1
        return neighbor_ls
    def decoding_single(self, feats_str):
        return int(feats_str.decode("utf-8"))
        # return int(feats_str)

    def create_input(self, data_step):
        src, nbrs, label, feats = data_step
        # print("src shape", src.shape)
        # print("nbrs shape", nbrs.shape)
        # print("label shape", label.shape)
        # print("feats shape", feats.shape)
        idx_map = {src.item():0}
        for idn in range(len(nbrs)):
            idx_map[nbrs[idn].item()] = idn + 1
        new_edges = torch.zeros((2, len(nbrs))).long()
        for idn in range(len(nbrs)):
            new_edges[1][idn] = idx_map[nbrs[idn].item()]
        # new_edges, label, feats
        mask_all = torch.tensor([0]).long()
        # labels = [label]
        new_data = PyG_Data(x = feats, edge_index = new_edges, y = label, train_mask = mask_all, val_mask = mask_all, test_mask = mask_all)
        # new_data = {'x': feats, 'edge_index': new_edges, 'y': label, 'mask_all': mask_all}
        return new_data
    def parse_record(self, record):
        with open('/opt/res.txt', 'w') as f:
          f.write(record)
        df = pd.read_csv(StringIO(record), names=["src", "edges"], encoding='utf8')
        df.to_csv('/opt/res.csv') 
        df['embed'] = df['embed'].apply(lambda x: bytes.fromhex(x))
        ############ For debugging only, remove this later ######################
        # print("we are reading the sample data from the file")
        # df = pd.read_csv('/opt/graphlearning/sample_data.csv')
        self.input_types = ["INT_64", "INT_64", "INT_64", "FLOAT_32"]
        print(df.columns)
        ############ For debugging only, remove this later ######################

        tensors = []
        for idx, key in enumerate(["src", "nbr", "label", "embed"]):
            if key == 'src':
                if isinstance(df[key][0], np.int64):
                    cur = torch.from_numpy(np.array([df[key][0]])).to(DL_ON_FLINK_TYPE_TO_PYTORCH_TYPE[self.input_types[idx]])
                else:
                    cur = torch.tensor([self.decoding_single(df[key][0])], dtype=DL_ON_FLINK_TYPE_TO_PYTORCH_TYPE[self.input_types[idx]])
            elif key == 'label':
                labels = np.zeros(self.node_num)
                labels[0] = df[key][0]
                cur = torch.from_numpy(labels).to(DL_ON_FLINK_TYPE_TO_PYTORCH_TYPE[self.input_types[idx]])
                # print("labels", labels)
            elif key == 'nbr':
                cur = torch.tensor(self.decoding_neighbors(df[key][0]), dtype=DL_ON_FLINK_TYPE_TO_PYTORCH_TYPE[self.input_types[idx]])
            elif key == 'embed':
                feats = self.decoding_features(df[key][0])
                feat_dtype = DL_ON_FLINK_TYPE_TO_PYTORCH_TYPE[self.input_types[idx]]
                feat_tensors = [torch.from_numpy(feat).unsqueeze(0).to(feat_dtype) for feat in feats]
                cur = torch.cat(feat_tensors)
            tensors.append(cur)
        pyg_data = self.create_input(tensors)
        return pyg_data
