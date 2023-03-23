import torch
import torch.nn.functional as F
from torch_geometric.data import Data


def train_step(model, optimizer, data_step, iter_num = 10):
    model.train()
    new_data = data_step
    
    for epoch in range(iter_num):
        optimizer.zero_grad()
        out = model(new_data)
        # out = model(new_data.x, new_data.edge_index, new_data.batch)
        loss = F.nll_loss(out[data_step.train_mask], new_data.y[data_step.train_mask])
        loss.backward()
        # loss_ls.append(loss.item())
        optimizer.step()
    return loss.item()

def val_step(model, data_step):
    model.eval()
    new_data = data_step
    out = model(new_data)
    # out = model(new_data.x, new_data.edge_index, new_data.batch)
    loss = F.nll_loss(out[data_step.val_mask], new_data.y[data_step.val_mask])

    return loss.item()

def infer_step(model, data_step):
    new_data = data_step
    model.eval()
    out = model(new_data)
    # out = model(new_data.x, new_data.edge_index, new_data.batch)

    return out[data_step.test_mask]
