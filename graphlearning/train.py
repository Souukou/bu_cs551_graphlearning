import logging
import os

import torch
import torch.distributed as dist
from dl_on_flink_framework.context import Context
from dl_on_flink_pytorch.pytorch_context import PyTorchContext
from new_pycontext import NewPyTorchContext
from pyflink.common import Row
from torch import nn
from torch.nn.parallel import DistributedDataParallel as DDP
from torch.optim import SGD
from model import GCN
from torch_geometric.loader import DataLoader as PyG_DataLoader
from torch.utils.data import DataLoader
from torch_geometric.data import Data as PyG_Data
import utils
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

def train(context: Context):
    # pytorch_context = PyTorchContext(context)
    pytorch_context = NewPyTorchContext(context)
    os.environ['MASTER_ADDR'] = pytorch_context.get_master_ip()
    os.environ['MASTER_PORT'] = str(pytorch_context.get_master_port())
    dist.init_process_group(backend='gloo',
                            world_size=pytorch_context.get_world_size(),
                            rank=pytorch_context.get_rank())
    # modify this later!!! use the PyG_DataLoader
    data_loader = PyG_DataLoader(pytorch_context.get_dataset_from_flink(), batch_size=1)

    model = DDP(GCN(17,41))
    optimizer = torch.optim.Adam(model.parameters(), lr=0.01, weight_decay=5e-4)

    #where to get the epoch?
    current_epoch = 99999
    logger.info(f"Epoch: {current_epoch}")
    for batch_idx, data in enumerate(data_loader):
        print("the data we are getting", data)
        # for key in data.keys():
        #     print(key, data[key].shape)
        # remove this to real batch operation if need
        # new_data = PyG_Data(x = data['x'][0], edge_index = data['edge_index'][0], y = data['y'][0], 
        #                         train_mask = data['mask_all'][0], val_mask = data['mask_all'][0], test_mask = data['mask_all'][0])
        # print("the new data we are getting", data)
        loss = utils.train_step(model, optimizer, data)
        print(batch_idx, loss)
        if batch_idx % 100 == 0:
            logger.info(
                f"rank: {pytorch_context.get_rank()} batch: {batch_idx} "
                f"loss: {loss:>7f}")

    if pytorch_context.get_rank() == 0:
        model_save_path = pytorch_context.get_property("model_save_path")
        os.makedirs(os.path.dirname(model_save_path), exist_ok=True)
        torch.save(model.module, model_save_path)
