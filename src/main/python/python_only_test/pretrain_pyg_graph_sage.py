import torch
from torch_geometric.utils import subgraph, k_hop_subgraph
from torch_geometric.datasets import Reddit, KarateClub, CoraFull, Planetoid
from torch_geometric.loader import NeighborLoader
from torch_geometric.loader import DataLoader as PyG_DataLoader
import copy
from tqdm import tqdm
import torch.nn.functional as F
from model import GS_model
import numpy as np


def pre_train(epoch, model, optimizer, train_loader, device):
    model.train()

    pbar = tqdm(total=int(len(train_loader.dataset)))
    pbar.set_description(f'Epoch {epoch:02d}')

    total_loss = total_correct = total_examples = 0
    for batch in train_loader:
        optimizer.zero_grad()
        batch.to(device)
        # print("batch", torch.sum(train_loader.data.train_mask))
        # print("batch index!!!", torch.unique(batch.edge_index))
        # print(batch.edge_index)
        # print(torch.sum(batch.x))
        # print(batch.x.shape)
        # print(batch.x.dtype)
        # print(batch.n_id)
        # print(batch.train_mask)
        # input()
        y = batch.y[:batch.batch_size]
        y_hat = model(batch.x, batch.edge_index.to(device))[:batch.batch_size]
        loss = F.cross_entropy(y_hat, y)
        loss.backward()
        optimizer.step()
        # print(loss)

        total_loss += float(loss) * batch.batch_size
        total_correct += int((y_hat.argmax(dim=-1) == y).sum())
        total_examples += batch.batch_size
        pbar.update(batch.batch_size)
    pbar.close()

    return total_loss / total_examples, total_correct / total_examples

@torch.no_grad()
def pre_test(model, test_loader, device):
    model.eval()

    pbar = tqdm(total=int(len(test_loader.dataset)))
    pbar.set_description(f'testing the pretraining model now!')

    total_correct = total_examples = 0
    for batch in test_loader:
        batch.to(device)
        
        y = batch.y[:batch.batch_size]
        y_hat = model(batch.x, batch.edge_index.to(device))[:batch.batch_size]
        # print(y)

        total_correct += int((y_hat.argmax(dim=-1) == y).sum())
        total_examples += batch.batch_size
        pbar.update(batch.batch_size)
    pbar.close()

    return total_correct / total_examples

def pre_test_whole(model, batch, device):
    model.eval()

    # pbar = tqdm(total=int(len(test_loader.dataset)))
    # pbar.set_description(f'testing the pretraining model now!')

    batch.to(device)    
    y = batch.y[batch.test_mask]
    y_hat = model(batch.x, batch.edge_index.to(device))[batch.test_mask]
    # print(y)

    total_correct = int((y_hat.argmax(dim=-1) == y).sum())
    total_examples = torch.sum(batch.test_mask)
    return total_correct / total_examples


device = torch.device('cuda:0' if torch.cuda.is_available() else 'cpu')
print(device)
path = './dataset'
# dataset = CoraFull(path)
dataset = Planetoid(path, "PubMed", split ='full')
print(dataset)
data = dataset[0]
print("data", data)
print(torch.sum(data.train_mask))
print(torch.sum(data.val_mask))
print(torch.sum(data.test_mask))
input()

# train_feats = data.x[data.train_mask]
# print(data.train_mask)
train_idxs = np.arange(data.num_nodes)[data.train_mask].tolist()
# train_idxs = np.arange(data.num_nodes).tolist()

new_idx = train_idxs[:int(len(train_idxs) * 0.2)]
print(new_idx)
print(data.x.shape)

print(data.edge_index.shape)
edge_index = data.edge_index
new_edge_index, _ = subgraph(new_idx, edge_index)
print(new_edge_index)
new_feature = data.x[new_idx]
new_label = data.y[new_idx]
print(new_feature.shape)
print(data.edge_index.shape)


new_data = copy.deepcopy(data)
# new_train_mask = copy.deepcopy(new_data.train_mask)
new_train_mask = np.array([False] * data.num_nodes)
new_test_mask = np.array([True] * data.num_nodes)
new_train_mask[new_idx] = True
new_test_mask[new_idx] = False
del new_data.edge_index, new_data.train_mask#, new_data.x, new_data.y

new_data.edge_index = new_edge_index
new_data.train_mask = torch.from_numpy(new_train_mask)
# new_data.train_mask = new_train_mask
# new_data.x = new_feature
# new_data.y = new_label
# new_data.train_mask = torch.tensor([True for _ in range(len(new_data.x))])
# print(new_data.train_mask.shape, new_data.x.shape, new_data.y.shape)
print(new_data)

# pretrain_dict = {'pt_edges':new_edge_index.numpy(), "pt_mask": new_train_mask.numpy()}
pretrain_dict = {'pt_edges':new_edge_index.numpy(), "pt_mask": new_train_mask}
pt_dict = './dataset/pretrain_dict.npy'
print("not saving new numpy dicts")

np.save(pt_dict, pretrain_dict)
print("saving the the pretrained dict to", pt_dict)

kwargs = {'batch_size': 4}

sub_train_loader = NeighborLoader(new_data, input_nodes=new_data.train_mask,
                                 num_neighbors=[2, 2], shuffle=True, **kwargs)
print("hello?")
# data.test_mask = torch.from_numpy(new_test_mask)
print(data.test_mask)

test_loader = NeighborLoader(data, num_neighbors=[-1],input_nodes=data.test_mask, shuffle=False, **kwargs)
print(dataset.num_features)
print(dataset.num_classes)
print(dataset.x.shape)
model = GS_model(dataset.num_features, 256, dataset.num_classes, 2).to(device)
optimizer = torch.optim.Adam(model.parameters(), lr=0.01)

for epoch in range(5):
    loss, acc = pre_train(epoch, model, optimizer, sub_train_loader, device)
    print("loss and acc", loss, acc)
    print("test acc", pre_test(model, test_loader, device))
    # print("test acc", pre_test_whole(model, data, device))
print(dataset.num_features, dataset.num_classes)

torch.save(model.state_dict(), './dataset/pretrianed_graph_sage.pth')
