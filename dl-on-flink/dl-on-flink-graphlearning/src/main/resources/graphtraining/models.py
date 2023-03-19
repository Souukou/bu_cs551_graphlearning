import torch
import torch_geometric


class GCN(torch.nn.Module):
  def __init__(self, in_features, out_features):
    self.layer1 = torch_geometric.nn.GCNConv(in_features, 16)
    self.layer2 = torch_geometric.nn.GCNConv(16, out_features)

  def forward(self data):
    x, edge_index = data.x, data.edge_index

    x = self.layer1(x, edge_index)
    x = self.layer2(x, edge_index)
    
