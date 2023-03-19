import torch

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