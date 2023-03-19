import logging
import os

import torch
import torch.distributed as dist
from dl_on_flink_framework.context import Context
from dl_on_flink_pytorch.pytorch_context import PyTorchContext
from pyflink.common import Row
from torch import nn
from torch.nn.parallel import DistributedDataParallel as DDP
from torch.optim import SGD
import dataloader
import utils

device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

def train(context: Context):
    pytorch_context = PyTorchContext(context)
    os.environ['MASTER_ADDR'] = pytorch_context.get_master_ip()
    os.environ['MASTER_PORT'] = str(pytorch_context.get_master_port())
    dist.init_process_group(backend='gloo',
                            world_size=pytorch_context.get_world_size(),
                            rank=pytorch_context.get_rank())
    data_loader = dataloader.get_dataloader(batch_size=128)

    model = DDP(Linear())
    optimizer = torch.optim.Adam(model.parameters(), lr=0.01, weight_decay=5e-4)

    logger.info(f"Epoch: {current_epoch}")
    for batch_idx, data in enumerate(data_loader):
        loss = utils.train_step(model, optimizer, data)
        if batch_idx % 100 == 0:
            logger.info(
                f"rank: {pytorch_context.get_rank()} batch: {batch_idx} "
                f"loss: {loss:>7f}")

    if pytorch_context.get_rank() == 0:
        model_save_path = pytorch_context.get_property("model_save_path")
        os.makedirs(os.path.dirname(model_save_path), exist_ok=True)
        torch.save(model.module, model_save_path)
