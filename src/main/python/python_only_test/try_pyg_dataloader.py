import torch
from torch_geometric.utils import subgraph, k_hop_subgraph
from torch_geometric.datasets import Reddit
from torch_geometric.loader import NeighborLoader
from torch_geometric.loader import DataLoader as PyG_DataLoader
import copy
import torch.nn.functional as F
import numpy as np

from torch.utils.data import IterableDataset
from torch_geometric.data import Data

def re_index_batch(batch):
    idx_map = {}
    cur_num = 0
    n_id = []
    edges = copy.deepcopy(batch.edge_index)
    print(batch.edge_index)
    input()
    for idn in range(len(edges[0])):
        # print(edges)
        node0 = edges[0][idn].item()
        # print(node0)
        if node0 not in idx_map:
            n_id.append(node0)
            idx_map[node0] = cur_num
            cur_num += 1
        edges[0][idn] = idx_map[node0]
    for idn in range(len(edges[1])):
        node1 = edges[1][idn].item()
        edges[1][idn] = idx_map[node1]
    all_mask = sum(batch.train_mask,[])
    new_mask = [oid in all_mask for oid in n_id]
    batch.edge_index = edges
    batch.n_id = n_id
    batch.train_mask = new_mask
    return batch

class TryStreamDataset(IterableDataset):
    """
    The Pytorch Dataset that reads data from Flink. The Dataset returns a list
    of PyTorch tensors. Each column of the Flink row is a tensor.
    """

    def __init__(self):
        self.iter_num_cus = 0

    def __iter__(self):
        # self.iter_num_cus = 0
        while True:
            # self.iter_num_cus = 0
            try:
                # res = self.java_file.read(4, not first_read)
                # first_read = False
                # data_len, = struct.unpack("<i", res)
                # record = self.java_file.read(data_len, True).decode('utf-8')
                # tensors = self.parse_record(record)
                if self.iter_num_cus % 2 ==0:
                    # n0 = self.iter_num_cus
                    # n1 = self.iter_num_cus + 1
                    # n2 = self.iter_num_cus + 2
                    n0 = 0
                    n1 = 1
                    n2 = 2
                    edge_index = torch.tensor([[n0, n1],
                            [n1, n0],
                            [n1, n2],
                            [n2, n1]], dtype=torch.long)
                    
                    x = torch.tensor([[n0], [n1], [n2]], dtype=torch.float)
                    y = torch.tensor([[n0], [n1], [n2]], dtype=torch.long)
                    nid = [self.iter_num_cus*100, self.iter_num_cus*100 + 1, self.iter_num_cus*100 + 2]
                else:
                    n0 = 0
                    n1 = 1
                    # n2 = self.iter_num_cus + 2
                    edge_index = torch.tensor([[n0, n1],
                            [n1, n0]], dtype=torch.long)
                    
                    x = torch.tensor([[n0], [n1]], dtype=torch.float)
                    y = torch.tensor([[n0], [n1]], dtype=torch.long)
                    nid = [self.iter_num_cus*100, self.iter_num_cus*100 + 1]

                mask_all = [n0]
                data = Data(x=x, edge_index=edge_index.t().contiguous(), y = y, train_mask = mask_all, val_mask = mask_all, test_mask = mask_all)
                data.nid = nid
                print("generating", data.edge_index)
                self.iter_num_cus += 1
                # del n0, n1, n2
                yield data
            except:
                break

dumy_dataset = TryStreamDataset()
# print(dumy_dataset[0].edge_index)
# print(dumy_dataset.next().edge_index)
# print(dumy_dataset.next().edge_index)
data_loader = PyG_DataLoader(dumy_dataset, batch_size=4)

for batch_idx, old_data in enumerate(data_loader):
    # data = re_index_batch(old_data)
    data = old_data
    print("the data we are getting", data)
    print("x", data.x)
    print("edge_index", data.edge_index)
    print("y", data.y)
    print("batch", data.batch)
    print("train_mask", data.train_mask)
    print("old node id", data.nid)
    input()
