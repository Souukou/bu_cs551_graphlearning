import torch
import torch.nn.functional as F
from torch_geometric.nn import GCNConv
from torch_geometric.nn.models import GraphSAGE


class GCN(torch.nn.Module):
    def __init__(self, num_feat, num_classes):
        super().__init__()
        self.conv1 = GCNConv(num_feat, 16)
        self.conv2 = GCNConv(16, num_classes)

    def forward(self, data):
        x, edge_index = data.x, data.edge_index

        x = self.conv1(x, edge_index)
        x = F.relu(x)
        x = F.dropout(x, training=self.training)
        x = self.conv2(x, edge_index)

        return F.log_softmax(x, dim=1)

class GS_model(torch.nn.Module):
    def __init__(self, num_feat,hid_num = 16, num_classes=10, num_layers = 3,dropout = 0.0, act = 'relu', norm = None, jk = None):
        super().__init__()
        self.net = GraphSAGE(num_feat, hid_num, num_layers=num_layers, out_channels=num_classes,
                      dropout=dropout, act=act, norm=norm, jk=jk)
    
    def forward(self, x, edge_index):
        #  = data.x, data.edge_index
        out = self.net(x, edge_index)
        return F.log_softmax(out, dim=1)