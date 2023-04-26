import torch
import torch.nn.functional as F
from torch_geometric.data import Data


def train_step(model, optimizer, batch, device = 'cpu'):
    model.train()
    optimizer.zero_grad()
    batch.to(device)
    train_mask = sum(batch.train_mask,[])
    y = batch.y[train_mask][:,0]
    out = model(batch.x, batch.edge_index.to(device))[train_mask]
    loss = F.cross_entropy(out, y)
    loss.backward()
    optimizer.step()
    return loss.item()

def val_step(model, batch, device = 'cpu'):
    model.eval()
    batch.to(device)
    with torch.no_grad():
        batch_size = len(batch.val_mask)
        valid_mask = sum(batch.val_mask,[])
        y = batch.y[valid_mask][:,0]
        out = model(batch.x, batch.edge_index.to(device))[valid_mask]
        loss = F.cross_entropy(out, y)
        acc = int((out.argmax(dim=-1) == y).sum()) / batch_size

    return loss.item(), acc

def infer_step(model, batch, device = 'cpu'):
    model.eval()
    batch.to(device)
    with torch.no_grad():
        test_mask = sum(batch.test_mask,[])
        out = model(batch.x, batch.edge_index.to(device))[test_mask]
        output = out.data.cpu().numpy().argmax(axis=1)

    return output